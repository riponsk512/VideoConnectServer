// index.js (Root of VideoConnectServer GitHub repository)
require("dotenv").config();
const express = require("express");
const http = require("http");
const cors = require("cors");
const crypto = require("crypto");
const { Server } = require("socket.io");

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
  },
  pingInterval: 5000,
  pingTimeout: 10000,
  transports: ["websocket", "polling"],
});

const PORT = process.env.PORT || 5000;
const QUEUE_MAX_WAIT_MS = 10000; // 10s max wait in matchmaking queue
const STALE_QUEUE_PING_MS = 7500; // Evict ghost queue users after 7.5s without ping
const CALL_KILL_GRACE_MS = 10000; // 10s grace for Wi-Fi <-> Cellular switch; if app was killed, ends call
const PROTECTED_AD_GRACE_MS = 25000; // 25s grace when user is picking an image or granting permissions
const CONNECTING_KILL_GRACE_MS = 8000; // 8s grace during connecting -> InCall screen transition

// ============================================================================
// 📝 STRUCTURED RENDER LOGGER
// ============================================================================
function logEvent(category, message, details = null) {
  const ts = new Date().toISOString();
  if (details !== null && details !== undefined) {
    console.log(
      `[${ts}] [${category}] ${message} ->`,
      typeof details === "object" ? JSON.stringify(details) : details
    );
  } else {
    console.log(`[${ts}] [${category}] ${message}`);
  }
}

// ============================================================================
// 🗂️ IN-MEMORY STORES
// ============================================================================
const waitingQueue = new Map(); // emailKey -> queueEntry
const activeRooms = new Map(); // roomId (20-char) -> roomObject
const userToSocket = new Map(); // emailKey -> socket.id
const socketToUser = new Map(); // socket.id -> emailKey
const userToRoom = new Map(); // emailKey -> roomId
const userProtectedUntil = new Map(); // emailKey -> timestamp
const matchingLocks = new Set(); // emailKey
const queueTimers = new Map(); // emailKey -> Timeout
const disconnectGraceTimers = new Map(); // emailKey -> Timeout

const ID_CHARS =
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
function generateRoomId(length = 20) {
  let result = "";
  const randomBytes = crypto.randomBytes(length);
  for (let i = 0; i < length; i++) {
    result += ID_CHARS[randomBytes[i] % ID_CHARS.length];
  }
  return activeRooms.has(result) ? generateRoomId(length) : result;
}

function clearQueueTimer(emailKey) {
  const timer = queueTimers.get(emailKey);
  if (timer) {
    clearTimeout(timer);
    queueTimers.delete(emailKey);
  }
}

function clearDisconnectGraceTimer(emailKey) {
  const timer = disconnectGraceTimers.get(emailKey);
  if (timer) {
    clearTimeout(timer);
    disconnectGraceTimers.delete(emailKey);
  }
}

function removeFromWaitingQueue(emailKey, reason = "removed") {
  if (!emailKey) return;
  clearQueueTimer(emailKey);
  if (waitingQueue.has(emailKey)) {
    waitingQueue.delete(emailKey);
    logEvent("QUEUE_REMOVE", `Removed ${emailKey} from waitingQueue`, {
      reason,
      remainingInQueue: waitingQueue.size,
    });
  }
  matchingLocks.delete(emailKey);
}

// ============================================================================
// 🔍 BIDIRECTIONAL FILTER & DAILY LIMIT COMPATIBILITY CHECK
// ============================================================================
function evaluateCompatibility(userA, userB) {
  const genderA = userA.gender ? String(userA.gender).toLowerCase() : null;
  const genderB = userB.gender ? String(userB.gender).toLowerCase() : null;
  const limitA = userA.dailyMatchLimit || 3;
  const limitB = userB.dailyMatchLimit || 3;

  if (!userA.isVip) {
    if (
      genderA === "male" &&
      genderB === "female" &&
      (userA.femaleConnectCount || 0) >= limitA
    ) {
      return {
        compatible: false,
        reason: `${userA.email} hit daily female limit (${limitA})`,
      };
    }
    if (
      genderA === "female" &&
      genderB === "male" &&
      (userA.maleConnectCount || 0) >= limitA
    ) {
      return {
        compatible: false,
        reason: `${userA.email} hit daily male limit (${limitA})`,
      };
    }
  }

  if (!userB.isVip) {
    if (
      genderB === "male" &&
      genderA === "female" &&
      (userB.femaleConnectCount || 0) >= limitB
    ) {
      return {
        compatible: false,
        reason: `${userB.email} hit daily female limit (${limitB})`,
      };
    }
    if (
      genderB === "female" &&
      genderA === "male" &&
      (userB.maleConnectCount || 0) >= limitB
    ) {
      return {
        compatible: false,
        reason: `${userB.email} hit daily male limit (${limitB})`,
      };
    }
  }

  const bHasFilters =
    userB.filterGender ||
    userB.filterLocation ||
    userB.filterAgeMin ||
    userB.filterAgeMax ||
    (Array.isArray(userB.filterMaritalStatus) &&
      userB.filterMaritalStatus.length > 0);

  const aMatchesB =
    !bHasFilters ||
    ((!userB.filterGender ||
      userB.filterGender === "Both" ||
      String(userB.filterGender).toLowerCase() === genderA) &&
      (!userB.filterLocation ||
        (userA.location &&
          String(userA.location).toLowerCase() ===
            String(userB.filterLocation).toLowerCase())) &&
      (!userB.filterMaritalStatus?.length ||
        userB.filterMaritalStatus.includes(userA.maritalStatus)) &&
      (!userB.filterAgeMin ||
        (userA.age && userA.age >= userB.filterAgeMin)) &&
      (!userB.filterAgeMax ||
        (userA.age && userA.age <= userB.filterAgeMax)));

  if (!aMatchesB) {
    return {
      compatible: false,
      reason: `${userA.email} did not match ${userB.email}'s filters`,
    };
  }

  const aHasFilters =
    userA.filterGender ||
    userA.filterLocation ||
    userA.filterAgeMin ||
    userA.filterAgeMax ||
    (Array.isArray(userA.filterMaritalStatus) &&
      userA.filterMaritalStatus.length > 0);

  const bMatchesA =
    !aHasFilters ||
    ((!userA.filterGender ||
      userA.filterGender === "Both" ||
      String(userA.filterGender).toLowerCase() === genderB) &&
      (!userA.filterLocation ||
        (userB.location &&
          String(userB.location).toLowerCase() ===
            String(userA.filterLocation).toLowerCase())) &&
      (!userA.filterMaritalStatus?.length ||
        userA.filterMaritalStatus.includes(userB.maritalStatus)) &&
      (!userA.filterAgeMin ||
        (userB.age && userB.age >= userA.filterAgeMin)) &&
      (!userA.filterAgeMax ||
        (userB.age && userB.age <= userA.filterAgeMax)));

  if (!bMatchesA) {
    return {
      compatible: false,
      reason: `${userB.email} did not match ${userA.email}'s filters`,
    };
  }

  return { compatible: true, reason: "Bidirectional filters matched" };
}

// ============================================================================
// 🧹 ROOM TEARDOWN & PARTNER NOTIFICATION HELPER
// ============================================================================
function destroyRoomAndNotifyPartner(
  roomId,
  leaverEmail,
  reason = "ended",
  forceConnectingRequeue = false
) {
  if (!roomId || !activeRooms.has(roomId)) return;
  const room = activeRooms.get(roomId);
  activeRooms.delete(roomId);

  const callerKey = String(room.caller || "").toLowerCase();
  const calleeKey = String(room.callee || "").toLowerCase();
  const leaverKey = String(leaverEmail || "").toLowerCase();

  clearDisconnectGraceTimer(callerKey);
  clearDisconnectGraceTimer(calleeKey);

  if (userToRoom.get(callerKey) === roomId) userToRoom.delete(callerKey);
  if (userToRoom.get(calleeKey) === roomId) userToRoom.delete(calleeKey);

  const partnerKey = leaverKey === callerKey ? calleeKey : callerKey;
  const partnerSocketId = userToSocket.get(partnerKey);

  const durationSec = Math.round(
    (Date.now() - (room.createdAtMs || Date.now())) / 1000
  );

  logEvent("ROOM_DESTROYED", `Room ${roomId} closed`, {
    roomId,
    caller: room.caller,
    callee: room.callee,
    endedBy: leaverEmail || "unknown",
    reason,
    durationSec: `${durationSec}s`,
    enteredInCall: room.enteredInCall,
    remainingActiveRooms: activeRooms.size,
  });

  if (!partnerSocketId) return;

  const isStillInConnectingHold =
    forceConnectingRequeue && !room.enteredInCall;

  if (isStillInConnectingHold) {
    io.to(partnerSocketId).emit("partner_disconnected_requeue", {
      roomId,
      action: reason,
      reason,
      by: leaverEmail,
      timestamp: Date.now(),
    });
  } else {
    const cleanReason = String(reason || "ended").toLowerCase();
    const isNext = cleanReason === "next";
    const isLost = cleanReason === "disconnected";

    io.to(partnerSocketId).emit("room_ended", {
      roomId,
      status: cleanReason,
      endedBy: leaverEmail,
      endedReason: cleanReason,
      timestamp: Date.now(),
    });

    io.to(partnerSocketId).emit("call_alert", {
      roomId,
      action: cleanReason,
      title: isNext
        ? "Stranger left"
        : isLost
        ? "Connection Lost ⚠️"
        : "Call ended",
      message: isNext
        ? "User is looking for someone else"
        : isLost
        ? "User has lost the connection"
        : "The user has ended the call",
      by: leaverEmail,
      timestamp: Date.now(),
    });
  }
}

// ============================================================================
// ⚡ SOCKET.IO CONNECTION & REAL-TIME SIGNALING ENGINE
// ============================================================================
io.on("connection", (socket) => {
  logEvent("SOCKET_CONNECTED", `New socket connected: ${socket.id}`);

  // 1. REGISTER USER
  socket.on("register_user", (payload) => {
    if (!payload?.email) return;
    const email = String(payload.email).trim();
    const emailKey = email.toLowerCase();

    clearDisconnectGraceTimer(emailKey);

    const prevSocketId = userToSocket.get(emailKey);
    if (prevSocketId && prevSocketId !== socket.id) {
      socketToUser.delete(prevSocketId);
    }

    userToSocket.set(emailKey, socket.id);
    socketToUser.set(socket.id, emailKey);

    const activeRoomId = payload.roomId || userToRoom.get(emailKey);
    if (activeRoomId && activeRooms.has(activeRoomId)) {
      socket.join(activeRoomId);
      const room = activeRooms.get(activeRoomId);
      room.lastHeartbeat = Date.now();
    }

    logEvent("USER_REGISTERED", `User registered: ${email}`, {
      socketId: socket.id,
      screen: payload.screen || "unknown",
      activeRoomId: activeRoomId || null,
    });
  });

  // 2. PROTECTION FLAG FOR GALLERY / PERMISSION DIALOGS
  socket.on("ad_status", (payload) => {
    const email = payload?.email || socketToUser.get(socket.id);
    if (!email) return;
    const key = String(email).toLowerCase();
    if (payload?.isActive) {
      userProtectedUntil.set(key, Date.now() + PROTECTED_AD_GRACE_MS);
    } else {
      userProtectedUntil.delete(key);
    }
  });

  // 3. JOIN MATCHMAKING QUEUE
  socket.on("queue_join", (payload) => {
    if (!payload?.email) return;
    const myEmail = String(payload.email).trim();
    const myKey = myEmail.toLowerCase();
    const now = Date.now();

    clearDisconnectGraceTimer(myKey);
    userToSocket.set(myKey, socket.id);
    socketToUser.set(socket.id, myKey);

    const existingRoomId = userToRoom.get(myKey);
    if (existingRoomId && activeRooms.has(existingRoomId)) {
      const existingRoom = activeRooms.get(existingRoomId);
      const roomAgeMs = now - (existingRoom.createdAtMs || 0);

      if (!payload.forceNewSearch && roomAgeMs < 4500) {
        const myRole =
          String(existingRoom.caller).toLowerCase() === myKey
            ? "caller"
            : "callee";
        const partnerEmail =
          myRole === "caller" ? existingRoom.callee : existingRoom.caller;

        logEvent(
          "QUEUE_JOIN_DEDUP",
          `Ignored duplicate queue_join for ${myEmail}; re-emitting active match ${existingRoomId}`
        );
        socket.join(existingRoomId);
        socket.emit("match_established_sync", {
          roomId: existingRoomId,
          role: myRole,
          partnerEmail,
          roomData: existingRoom,
        });
        return;
      }

      destroyRoomAndNotifyPartner(existingRoomId, myEmail, "left", false);
    }

    if (matchingLocks.has(myKey)) return;
    matchingLocks.add(myKey);

    const myEntry = {
      socketId: socket.id,
      email: myEmail,
      uid: payload.uid || null,
      mode: payload.mode || "video",
      age: Number(payload.age) || null,
      gender: payload.gender || null,
      location: payload.location || "INDIA",
      maritalStatus: payload.maritalStatus || null,
      filterGender: payload.filterGender || null,
      filterLocation: payload.filterLocation || null,
      filterMaritalStatus: Array.isArray(payload.filterMaritalStatus)
        ? payload.filterMaritalStatus
        : [],
      filterAgeMin: Number(payload.filterAgeMin) || null,
      filterAgeMax: Number(payload.filterAgeMax) || null,
      isVip: Boolean(payload.isVip),
      femaleConnectCount: Number(payload.femaleConnectCount) || 0,
      maleConnectCount: Number(payload.maleConnectCount) || 0,
      dailyMatchLimit: Number(payload.dailyMatchLimit) || 3,
      joinedAt: now,
      lastPing: now,
    };

    logEvent("QUEUE_JOIN", `🔍 User searching: ${myEmail}`, {
      gender: myEntry.gender,
      age: myEntry.age,
      location: myEntry.location,
      isVip: myEntry.isVip,
      waitingCountBeforeMatch: waitingQueue.size,
    });

    let matchedPartner = null;

    for (const [candKey, candidate] of waitingQueue.entries()) {
      if (candKey === myKey) continue;
      if (matchingLocks.has(candKey)) continue;
      if (userToRoom.has(candKey)) {
        removeFromWaitingQueue(candKey, "already_in_room");
        continue;
      }

      const candSocket = io.sockets.sockets.get(candidate.socketId);
      if (
        !candSocket ||
        !candSocket.connected ||
        now - candidate.lastPing > STALE_QUEUE_PING_MS
      ) {
        removeFromWaitingQueue(candKey, "stale_or_disconnected_socket");
        continue;
      }

      const check = evaluateCompatibility(myEntry, candidate);
      if (check.compatible) {
        matchingLocks.add(candKey);
        matchedPartner = candidate;
        removeFromWaitingQueue(candKey, "matched");
        break;
      } else {
        logEvent(
          "QUEUE_SKIP",
          `Skipped ${myEmail} <-> ${candidate.email}: ${check.reason}`
        );
      }
    }

    if (matchedPartner) {
      const partnerKey = matchedPartner.email.toLowerCase();
      removeFromWaitingQueue(myKey, "matched");
      removeFromWaitingQueue(partnerKey, "matched");

      const roomId = generateRoomId(20);
      const waitTimeMs = now - matchedPartner.joinedAt;

      const roomObject = {
        roomId,
        caller: matchedPartner.email,
        callee: myEntry.email,
        callerUid: matchedPartner.uid || null,
        calleeUid: myEntry.uid || null,
        callerCountry: String(matchedPartner.location || "INDIA").toUpperCase(),
        calleeCountry: String(myEntry.location || "INDIA").toUpperCase(),
        callerMuted: false,
        calleeMuted: false,
        callerVideoOff: false,
        calleeVideoOff: false,
        callerFilter: 0,
        calleeFilter: 0,
        callerPrivacy: false,
        calleePrivacy: false,
        callerQuality: "240 kbps • 360p",
        calleeQuality: "240 kbps • 360p",
        offer: null,
        answer: null,
        callerCandidates: [],
        calleeCandidates: [],
        iceRestartOffer: null,
        iceRestartAnswer: null,
        iceRestartRequest: null,
        lastGift: null,
        lastHeartbeat: now,
        createdAt: new Date(now).toISOString(),
        createdAtMs: now,
        enteredInCall: false,
        status: "active",
      };

      activeRooms.set(roomId, roomObject);
      userToRoom.set(partnerKey, roomId);
      userToRoom.set(myKey, roomId);

      const callerSocket = io.sockets.sockets.get(matchedPartner.socketId);
      if (callerSocket) callerSocket.join(roomId);
      socket.join(roomId);

      logEvent(
        "MATCH_ESTABLISHED",
        `🎉 MATCHED: ${matchedPartner.email} (Caller) <-> ${myEntry.email} (Callee)`,
        {
          roomId,
          partnerWaitTimeMs: `${waitTimeMs}ms`,
          activeRoomsTotal: activeRooms.size,
        }
      );

      io.to(matchedPartner.socketId).emit("match_established_sync", {
        roomId,
        role: "caller",
        partnerEmail: myEntry.email,
        partnerGender: myEntry.gender,
        roomData: roomObject,
      });

      io.to(myEntry.socketId).emit("match_established_sync", {
        roomId,
        role: "callee",
        partnerEmail: matchedPartner.email,
        partnerGender: matchedPartner.gender,
        roomData: roomObject,
      });

      matchingLocks.delete(myKey);
      matchingLocks.delete(partnerKey);
      return;
    }

    waitingQueue.set(myKey, myEntry);
    matchingLocks.delete(myKey);

    logEvent("QUEUE_WAITING", `⏳ ${myEmail} waiting in queue`, {
      waitingCount: waitingQueue.size,
      waitingUsers: Array.from(waitingQueue.keys()),
    });

    clearQueueTimer(myKey);
    const expiryTimer = setTimeout(() => {
      if (waitingQueue.has(myKey) && !userToRoom.has(myKey)) {
        removeFromWaitingQueue(myKey, "10s_timeout");
        logEvent("QUEUE_TIMEOUT", `⏰ 10s timeout for ${myEmail}`);
        socket.emit("queue_expired_10s", {
          email: myEmail,
          timestamp: Date.now(),
        });
      }
    }, QUEUE_MAX_WAIT_MS);
    queueTimers.set(myKey, expiryTimer);
  });

  // 4. 2-SECOND QUEUE PING
  socket.on("queue_ping", (payload) => {
    if (!payload?.email) return;
    const key = String(payload.email).trim().toLowerCase();
    clearDisconnectGraceTimer(key);
    userToSocket.set(key, socket.id);
    socketToUser.set(socket.id, key);

    const entry = waitingQueue.get(key);
    if (entry) {
      entry.lastPing = Date.now();
      entry.socketId = socket.id;
    }
    socket.emit("queue_pong", { timestamp: Date.now() });
  });

  // 5. LEAVE QUEUE / ABORT
  socket.on("queue_leave", (payload) => {
    const email = payload?.email || socketToUser.get(socket.id);
    if (!email) return;
    const key = String(email).trim().toLowerCase();
    removeFromWaitingQueue(key, payload?.reason || "queue_leave");

    if (payload?.explicitAbort === true) {
      const roomId = payload.roomId || userToRoom.get(key);
      if (roomId) {
        logEvent(
          "ABORT_MATCH",
          `🛑 ${email} explicitly aborted room ${roomId}`,
          { reason: payload.reason }
        );
        destroyRoomAndNotifyPartner(
          roomId,
          email,
          payload.dropStatus || payload.reason || "left",
          true
        );
      }
    }
  });

  // 6. ENTERED INCALL SCREEN
  socket.on("entered_incall", (payload) => {
    const { roomId, email, role } = payload || {};
    if (email) {
      const key = String(email).trim().toLowerCase();
      clearDisconnectGraceTimer(key);
      userToSocket.set(key, socket.id);
      socketToUser.set(socket.id, key);
    }
    if (roomId && activeRooms.has(roomId)) {
      const room = activeRooms.get(roomId);
      room.enteredInCall = true;
      room.lastHeartbeat = Date.now();
      socket.join(roomId);
      logEvent("ENTERED_INCALL", `🚀 ${email} (${role}) entered InCallScreen`, {
        roomId,
      });
    }
  });

  // ==========================================================================
  // 🎥 INCALL WEBRTC SIGNALING & STATE SYNC
  // ==========================================================================
  socket.on("join_room", (payload, callback) => {
    const { roomId, email } = payload || {};
    if (!roomId) return;
    socket.join(roomId);

    if (email) {
      const key = String(email).trim().toLowerCase();
      clearDisconnectGraceTimer(key);
      userToSocket.set(key, socket.id);
      socketToUser.set(socket.id, key);
      userToRoom.set(key, roomId);
    }

    const room = activeRooms.get(roomId) || null;
    if (room) {
      room.enteredInCall = true;
      room.lastHeartbeat = Date.now();
    }

    if (typeof callback === "function") {
      callback({ exists: Boolean(room), room });
    }

    if (room) {
      socket.emit("room_snapshot", room);

      const isCallee =
        email &&
        String(room.callee || "").toLowerCase() === String(email).toLowerCase();
      const isCaller =
        email &&
        String(room.caller || "").toLowerCase() === String(email).toLowerCase();

      if (isCallee && room.offer) {
        socket.emit("webrtc_offer", { roomId, offer: room.offer });
      }
      if (isCallee && room.callerCandidates.length > 0) {
        room.callerCandidates.forEach((candidate) => {
          socket.emit("webrtc_ice_candidate", {
            roomId,
            role: "caller",
            candidate,
          });
        });
      }
      if (isCaller && room.answer) {
        socket.emit("webrtc_answer", { roomId, answer: room.answer });
      }
      if (isCaller && room.calleeCandidates.length > 0) {
        room.calleeCandidates.forEach((candidate) => {
          socket.emit("webrtc_ice_candidate", {
            roomId,
            role: "callee",
            candidate,
          });
        });
      }
    }
  });

  socket.on("room_update", (payload) => {
    const { roomId, updates } = payload || {};
    if (!roomId || !updates || !activeRooms.has(roomId)) return;

    const room = activeRooms.get(roomId);
    Object.assign(room, updates, { lastHeartbeat: Date.now() });
    activeRooms.set(roomId, room);

    io.to(roomId).emit("room_snapshot", room);

    if (
      updates.status &&
      ["ended", "next", "left", "disconnected", "background"].includes(
        String(updates.status).toLowerCase()
      )
    ) {
      destroyRoomAndNotifyPartner(
        roomId,
        updates.endedBy || socketToUser.get(socket.id),
        updates.status,
        false
      );
    }
  });

  socket.on("webrtc_offer", (payload) => {
    const { roomId, offer } = payload || {};
    if (!roomId || !offer || !activeRooms.has(roomId)) return;
    socket.to(roomId).emit("webrtc_offer", { roomId, offer });

    const room = activeRooms.get(roomId);
    room.offer = offer;
    room.lastHeartbeat = Date.now();
    logEvent("WEBRTC_OFFER", `📡 Offer relayed in room ${roomId}`);
  });

  socket.on("webrtc_answer", (payload) => {
    const { roomId, answer } = payload || {};
    if (!roomId || !answer || !activeRooms.has(roomId)) return;
    socket.to(roomId).emit("webrtc_answer", { roomId, answer });

    const room = activeRooms.get(roomId);
    room.answer = answer;
    room.lastHeartbeat = Date.now();
    logEvent(
      "WEBRTC_ANSWER",
      `✅ Answer relayed in room ${roomId} — Call P2P Connected!`
    );
  });

  socket.on("webrtc_ice_candidate", (payload) => {
    const { roomId, role, candidate } = payload || {};
    if (!roomId || !candidate || !activeRooms.has(roomId)) return;
    socket.to(roomId).emit("webrtc_ice_candidate", {
      roomId,
      role,
      candidate,
    });

    const room = activeRooms.get(roomId);
    if (String(role).toLowerCase() === "caller") {
      room.callerCandidates.push(candidate);
    } else {
      room.calleeCandidates.push(candidate);
    }
    room.lastHeartbeat = Date.now();
  });

  socket.on("webrtc_ice_restart", (payload) => {
    const { roomId, iceRestartRequest, iceRestartOffer, iceRestartAnswer } =
      payload || {};
    if (!roomId || !activeRooms.has(roomId)) return;
    socket.to(roomId).emit("webrtc_ice_restart", payload);

    const room = activeRooms.get(roomId);
    if (iceRestartRequest !== undefined)
      room.iceRestartRequest = iceRestartRequest;
    if (iceRestartOffer !== undefined) room.iceRestartOffer = iceRestartOffer;
    if (iceRestartAnswer !== undefined)
      room.iceRestartAnswer = iceRestartAnswer;
    room.lastHeartbeat = Date.now();
  });

  socket.on("room_heartbeat", (payload) => {
    const { roomId } = payload || {};
    if (!roomId || !activeRooms.has(roomId)) return;
    const room = activeRooms.get(roomId);
    room.lastHeartbeat = Date.now();
  });

  const handleExplicitExit = (defaultReason) => (payload) => {
    const email =
      (typeof payload === "object" && payload?.email) ||
      socketToUser.get(socket.id);
    const emailKey = email ? String(email).toLowerCase() : null;
    if (emailKey) removeFromWaitingQueue(emailKey, defaultReason);

    const roomId =
      typeof payload === "string"
        ? payload
        : payload?.roomId || (emailKey ? userToRoom.get(emailKey) : null);

    if (roomId) {
      const reason =
        (typeof payload === "object" && (payload?.reason || payload?.status)) ||
        defaultReason;
      logEvent(
        "EXPLICIT_EXIT",
        `🚪 ${email || socket.id} exited room ${roomId} (${reason})`
      );
      destroyRoomAndNotifyPartner(
        roomId,
        email,
        reason,
        Boolean(payload?.isConnecting)
      );
    }
  };

  socket.on("leave_room", handleExplicitExit("ended"));
  socket.on("videoend", handleExplicitExit("ended"));
  socket.on("videonext", handleExplicitExit("next"));
  socket.on("videobackground", handleExplicitExit("background"));
  socket.on("checkInternet", handleExplicitExit("disconnected"));

  socket.on("disconnect", (disconnectReason) => {
    const emailKey = socketToUser.get(socket.id);
    if (!emailKey) return;

    socketToUser.delete(socket.id);
    if (userToSocket.get(emailKey) === socket.id) {
      userToSocket.delete(emailKey);
    }

    removeFromWaitingQueue(emailKey, `socket_disconnect:${disconnectReason}`);

    const activeRoomId = userToRoom.get(emailKey);
    if (!activeRoomId || !activeRooms.has(activeRoomId)) return;

    const room = activeRooms.get(activeRoomId);
    const isProtected = (userProtectedUntil.get(emailKey) || 0) > Date.now();

    const graceMs = isProtected
      ? PROTECTED_AD_GRACE_MS
      : room.enteredInCall
      ? CALL_KILL_GRACE_MS
      : CONNECTING_KILL_GRACE_MS;

    clearDisconnectGraceTimer(emailKey);
    const timer = setTimeout(() => {
      disconnectGraceTimers.delete(emailKey);
      const currentSocketId = userToSocket.get(emailKey);
      const currentSocket = currentSocketId
        ? io.sockets.sockets.get(currentSocketId)
        : null;

      if (!currentSocket || !currentSocket.connected) {
        logEvent(
          "APP_KILL_DETECTED",
          `💀 ${emailKey} did not reconnect after ${graceMs}ms. Closing room ${activeRoomId}`
        );
        destroyRoomAndNotifyPartner(
          activeRoomId,
          emailKey,
          "disconnected",
          !room.enteredInCall
        );
      }
    }, graceMs);

    disconnectGraceTimers.set(emailKey, timer);
  });
});

setInterval(() => {
  const now = Date.now();
  for (const [roomId, room] of activeRooms.entries()) {
    if (now - (room.lastHeartbeat || now) > 120000) {
      destroyRoomAndNotifyPartner(roomId, room.caller, "disconnected", false);
    }
  }
}, 60000);

app.get("/", (req, res) => {
  res.status(200).json({
    server: "VideoConnectServer",
    status: "online",
    waitingInQueue: waitingQueue.size,
    waitingUsers: Array.from(waitingQueue.keys()),
    activeRoomsCount: activeRooms.size,
    activeRoomIds: Array.from(activeRooms.keys()),
    uptimeSeconds: Math.floor(process.uptime()),
  });
});

app.get("/health", (req, res) => {
  res.status(200).send("OK");
});

server.listen(PORT, "0.0.0.0", () => {
  logEvent(
    "SERVER_START",
    `🚀 VideoConnectServer running on 0.0.0.0:${PORT}`
  );
});