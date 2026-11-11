// VideoConnectServer/index.js
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
  pingInterval: 4000,
  pingTimeout: 7000,
  transports: ["websocket", "polling"],
});

const PORT = process.env.PORT || 5000;
const QUEUE_MAX_WAIT_MS = 10000; // Strict 10s max wait in matchmaking queue
const STALE_QUEUE_PING_MS = 3800; // Auto-evict searching user if pings stop for >3.8s (data expired / net off)
const CALL_NETWORK_SWITCH_GRACE_MS = 8000; // 8s grace for Wi-Fi <-> Cellular switch during active call
const PROTECTED_AD_GRACE_MS = 25000; // 25s grace when user opens gallery or Bluetooth permission prompt

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
// 🗂️ IN-MEMORY STORES (ZERO DATABASE READS / WRITES)
// ============================================================================
const waitingQueue = new Map(); // emailKey -> queueEntry
const activeRooms = new Map(); // roomId (20-char) -> roomObject
const userToSocket = new Map(); // emailKey -> socket.id
const socketToUser = new Map(); // socket.id -> emailKey
const userToRoom = new Map(); // emailKey -> roomId
const userProtectedUntil = new Map(); // emailKey -> timestamp
const matchingLocks = new Set(); // emailKey (atomic lock preventing >2 user collisions)
const queueTimers = new Map(); // emailKey -> Timeout (strict 10s timer)
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

// 🔒 ZOMBIE-SAFE SOCKET BINDER
function bindUserSocket(email, socket) {
  if (!email || !socket) return null;
  const emailKey = String(email).trim().toLowerCase();

  clearDisconnectGraceTimer(emailKey);

  const prevSocketId = userToSocket.get(emailKey);
  if (prevSocketId && prevSocketId !== socket.id) {
    socketToUser.delete(prevSocketId);
    const oldSock = io.sockets.sockets.get(prevSocketId);
    if (oldSock) {
      try {
        oldSock.disconnect(true);
      } catch (e) {}
    }
  }

  userToSocket.set(emailKey, socket.id);
  socketToUser.set(socket.id, emailKey);

  const qEntry = waitingQueue.get(emailKey);
  if (qEntry) {
    qEntry.socketId = socket.id;
    qEntry.lastPing = Date.now();
  }

  const activeRoomId = userToRoom.get(emailKey);
  if (activeRoomId && activeRooms.has(activeRoomId)) {
    socket.join(activeRoomId);
    const room = activeRooms.get(activeRoomId);
    room.lastHeartbeat = Date.now();
  }

  return emailKey;
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
// 🧹 ROOM TEARDOWN & PARTNER NOTIFICATION HELPER (HANDLES BOTH HOLD & INCALL)
// ============================================================================
function destroyRoomAndNotifyPartner(roomId, leaverEmail, reason = "ended") {
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
  const partnerEmail = leaverKey === callerKey ? room.callee : room.caller;
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
    callerInCall: Boolean(room.callerEnteredInCall),
    calleeInCall: Boolean(room.calleeEnteredInCall),
    remainingActiveRooms: activeRooms.size,
  });

  if (!partnerSocketId) return;

  const cleanReason = String(reason || "ended").toLowerCase();
  const isNext = cleanReason === "next";
  const isLost =
    cleanReason === "disconnected" || cleanReason.includes("internet");

  // Always emit BOTH connecting-screen and incall-screen drop events so whichever
  // screen the partner is currently on (2.5s hold OR InCallScreen), they are alerted at 0ms!
  io.to(partnerSocketId).emit("partner_disconnected_requeue", {
    roomId,
    action: cleanReason,
    reason: cleanReason,
    by: leaverEmail,
    timestamp: Date.now(),
  });

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
      : "User Has Left",
    message: isNext
      ? "User is looking for someone else"
      : isLost
      ? "User has lost the connection"
      : "The user has left the connection",
    by: leaverEmail,
    timestamp: Date.now(),
  });

  logEvent("PARTNER_NOTIFIED", `Notified ${partnerEmail} of room teardown`, {
    roomId,
    reason: cleanReason,
  });
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
    const emailKey = bindUserSocket(email, socket);

    const activeRoomId = payload.roomId || userToRoom.get(emailKey);
    if (activeRoomId && activeRooms.has(activeRoomId)) {
      socket.join(activeRoomId);
    }

    logEvent("USER_REGISTERED", `User registered: ${email}`, {
      socketId: socket.id,
      screen: payload.screen || "unknown",
      activeRoomId: activeRoomId || null,
    });
  });

  // 2. GALLERY / BLUETOOTH PERMISSION PROTECTION FLAG
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

  // 3. JOIN MATCHMAKING QUEUE (Strict 1-to-1 Atomic Match + 10s Auto-Remove)
  socket.on("queue_join", (payload) => {
    if (!payload?.email) return;
    const myEmail = String(payload.email).trim();
    const myKey = bindUserSocket(myEmail, socket);
    const now = Date.now();

    // Protect active match if duplicate queue_join arrives within 5 seconds of matching
    const existingRoomId = userToRoom.get(myKey);
    if (existingRoomId && activeRooms.has(existingRoomId)) {
      const existingRoom = activeRooms.get(existingRoomId);
      const roomAgeMs = now - (existingRoom.createdAtMs || 0);

      if (!payload.forceNewSearch && roomAgeMs < 5000) {
        const myRole =
          String(existingRoom.caller).toLowerCase() === myKey
            ? "caller"
            : "callee";
        const partnerProfile =
          myRole === "caller"
            ? existingRoom.calleeProfile
            : existingRoom.callerProfile;

        socket.join(existingRoomId);
        socket.emit("match_established_sync", {
          roomId: existingRoomId,
          role: myRole,
          partnerEmail: partnerProfile.email,
          partnerGender: partnerProfile.gender,
          partnerProfile,
          roomData: existingRoom,
        });
        return;
      }

      // If starting a manual retry/new search, clean up any old room first
      destroyRoomAndNotifyPartner(existingRoomId, myEmail, "left");
    }

    if (matchingLocks.has(myKey)) return;
    matchingLocks.add(myKey);

    const myCountry = String(payload.location || "INDIA")
      .trim()
      .toUpperCase();
    const myEntry = {
      socketId: socket.id,
      email: myEmail,
      uid: payload.uid || null,
      name: payload.name || "Stranger",
      timeZone: payload.timeZone || "Asia/Kolkata",
      mode: payload.mode || "video",
      age: Number(payload.age) || null,
      gender: payload.gender || null,
      location: myCountry,
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
      name: myEntry.name,
      gender: myEntry.gender,
      age: myEntry.age,
      location: myEntry.location,
      isVip: myEntry.isVip,
      waitingInQueue: waitingQueue.size,
    });

    let matchedPartner = null;

    // Scan waitingQueue for exactly ONE compatible partner
    for (const [candKey, candidate] of waitingQueue.entries()) {
      if (candKey === myKey) continue;
      if (matchingLocks.has(candKey)) continue;
      if (userToRoom.has(candKey)) {
        removeFromWaitingQueue(candKey, "already_in_room");
        continue;
      }

      const candSocketId = userToSocket.get(candKey) || candidate.socketId;
      const candSocket = io.sockets.sockets.get(candSocketId);

      // Evict candidate immediately if their socket disconnected, ping expired, or 10s elapsed
      if (
        !candSocket ||
        !candSocket.connected ||
        now - candidate.lastPing > STALE_QUEUE_PING_MS ||
        now - candidate.joinedAt >= QUEUE_MAX_WAIT_MS
      ) {
        removeFromWaitingQueue(candKey, "stale_or_expired_candidate");
        continue;
      }

      const check = evaluateCompatibility(myEntry, candidate);
      if (check.compatible) {
        // 🔒 ATOMIC 1-TO-1 LOCK: Lock & remove candidate from queue immediately so no 3rd user can touch them
        matchingLocks.add(candKey);
        removeFromWaitingQueue(candKey, "matched_1_to_1");
        matchedPartner = { ...candidate, socketId: candSocketId };
        break;
      } else {
        logEvent(
          "QUEUE_SKIP",
          `Skipped ${myEmail} <-> ${candidate.email}: ${check.reason}`
        );
      }
    }

    // 🎉 1-TO-1 MATCH FOUND: Create 20-Char Room in RAM & Emit to Both Users Simultaneously
    if (matchedPartner) {
      const partnerKey = matchedPartner.email.toLowerCase();
      removeFromWaitingQueue(myKey, "matched_1_to_1");
      removeFromWaitingQueue(partnerKey, "matched_1_to_1");

      const roomId = generateRoomId(20);
      const waitTimeMs = now - matchedPartner.joinedAt;

      const callerProfile = {
        email: matchedPartner.email,
        uid: matchedPartner.uid || null,
        name: matchedPartner.name || "Stranger",
        gender: matchedPartner.gender || null,
        country: matchedPartner.location || "INDIA",
        timeZone: matchedPartner.timeZone || "Asia/Kolkata",
        isVip: Boolean(matchedPartner.isVip),
      };

      const calleeProfile = {
        email: myEntry.email,
        uid: myEntry.uid || null,
        name: myEntry.name || "Stranger",
        gender: myEntry.gender || null,
        country: myEntry.location || "INDIA",
        timeZone: myEntry.timeZone || "Asia/Kolkata",
        isVip: Boolean(myEntry.isVip),
      };

      const roomObject = {
        roomId,
        caller: matchedPartner.email,
        callee: myEntry.email,
        callerUid: matchedPartner.uid || null,
        calleeUid: myEntry.uid || null,
        callerName: callerProfile.name,
        calleeName: calleeProfile.name,
        callerTimeZone: callerProfile.timeZone,
        calleeTimeZone: calleeProfile.timeZone,
        callerIsVip: callerProfile.isVip,
        calleeIsVip: calleeProfile.isVip,
        callerCountry: callerProfile.country,
        calleeCountry: calleeProfile.country,
        callerProfile,
        calleeProfile,
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
        callerEnteredInCall: false,
        calleeEnteredInCall: false,
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
        `🎉 1-TO-1 MATCH: ${matchedPartner.email} (Caller) <-> ${myEntry.email} (Callee)`,
        {
          roomId,
          partnerWaitTimeMs: `${waitTimeMs}ms`,
          activeRoomsTotal: activeRooms.size,
          remainingQueueSize: waitingQueue.size,
        }
      );

      // Emit to Caller with full Callee profile
      io.to(matchedPartner.socketId).emit("match_established_sync", {
        roomId,
        role: "caller",
        partnerEmail: calleeProfile.email,
        partnerGender: calleeProfile.gender,
        partnerProfile: calleeProfile,
        roomData: roomObject,
      });

      // Emit to Callee with full Caller profile
      io.to(myEntry.socketId).emit("match_established_sync", {
        roomId,
        role: "callee",
        partnerEmail: callerProfile.email,
        partnerGender: callerProfile.gender,
        partnerProfile: callerProfile,
        roomData: roomObject,
      });

      matchingLocks.delete(myKey);
      matchingLocks.delete(partnerKey);
      return;
    }

    // No match yet -> Place user in waitingQueue with strict 10-second auto-removal
    waitingQueue.set(myKey, myEntry);
    matchingLocks.delete(myKey);

    logEvent("QUEUE_WAITING", `⏳ ${myEmail} added to waitingQueue (10s max)`, {
      waitingCount: waitingQueue.size,
      waitingUsers: Array.from(waitingQueue.keys()),
    });

    clearQueueTimer(myKey);
    const expiryTimer = setTimeout(() => {
      if (waitingQueue.has(myKey) && !userToRoom.has(myKey)) {
        removeFromWaitingQueue(myKey, "10s_timeout");
        logEvent(
          "QUEUE_TIMEOUT_10S",
          `⏰ 10s elapsed — auto-removed ${myEmail} from waitingQueue`
        );
        const curSockId = userToSocket.get(myKey) || socket.id;
        io.to(curSockId).emit("queue_expired_10s", {
          email: myEmail,
          timestamp: Date.now(),
        });
      }
    }, QUEUE_MAX_WAIT_MS);
    queueTimers.set(myKey, expiryTimer);
  });

  // 4. 2-SECOND QUEUE & HOLD PING
  socket.on("queue_ping", (payload) => {
    if (!payload?.email) return;
    const key = bindUserSocket(payload.email, socket);
    if (payload.roomId && activeRooms.has(payload.roomId)) {
      const room = activeRooms.get(payload.roomId);
      room.lastHeartbeat = Date.now();
    }
    socket.emit("queue_pong", { timestamp: Date.now() });
  });

  // 5. LEAVE QUEUE / ABORT DURING 2.5s HOLD
  socket.on("queue_leave", (payload) => {
    const email = payload?.email || socketToUser.get(socket.id);
    if (!email) return;
    const key = String(email).trim().toLowerCase();
    removeFromWaitingQueue(key, payload?.reason || "queue_leave");

    if (payload?.explicitAbort === true) {
      const roomId = payload.roomId || userToRoom.get(key);
      if (roomId) {
        logEvent(
          "ABORT_DURING_HOLD",
          `🛑 ${email} aborted/left room ${roomId} (${payload.reason})`
        );
        destroyRoomAndNotifyPartner(
          roomId,
          email,
          payload.dropStatus || payload.reason || "left"
        );
      }
    }
  });

  // 6. ENTERED INCALL SCREEN AFTER 2.5s HOLD
  socket.on("entered_incall", (payload) => {
    const { roomId, email, role } = payload || {};
    const key = email ? bindUserSocket(email, socket) : null;

    if (!roomId || !activeRooms.has(roomId)) {
      // Room was already destroyed because partner left during the 2.5s hold!
      socket.emit("room_ended", {
        roomId,
        status: "left",
        endedReason: "left",
      });
      return;
    }

    const room = activeRooms.get(roomId);
    if (String(role).toLowerCase() === "caller" || key === String(room.caller).toLowerCase()) {
      room.callerEnteredInCall = true;
    } else {
      room.calleeEnteredInCall = true;
    }
    room.enteredInCall = Boolean(
      room.callerEnteredInCall && room.calleeEnteredInCall
    );
    room.lastHeartbeat = Date.now();
    socket.join(roomId);

    logEvent("ENTERED_INCALL", `🚀 ${email} (${role}) entered InCallScreen`, {
      roomId,
      bothEntered: room.enteredInCall,
    });
  });

  // ==========================================================================
  // 🎥 INCALL WEBRTC SIGNALING & STATE SYNC (STRICT 2-USER GUARD)
  // ==========================================================================
  socket.on("join_room", (payload, callback) => {
    const { roomId, email } = payload || {};
    if (!roomId) return;

    const room = activeRooms.get(roomId) || null;
    if (!room) {
      if (typeof callback === "function") {
        callback({ exists: false, room: null });
      }
      socket.emit("room_ended", { roomId, status: "left" });
      return;
    }

    const emailKey = email ? bindUserSocket(email, socket) : null;
    const callerKey = String(room.caller || "").toLowerCase();
    const calleeKey = String(room.callee || "").toLowerCase();

    // Strict 1-to-1 Room Guard: Only the paired Caller and Callee may join this room
    if (emailKey && emailKey !== callerKey && emailKey !== calleeKey) {
      if (typeof callback === "function") {
        callback({ exists: false, room: null });
      }
      return;
    }

    socket.join(roomId);
    if (emailKey) {
      userToRoom.set(emailKey, roomId);
      if (emailKey === callerKey) room.callerEnteredInCall = true;
      if (emailKey === calleeKey) room.calleeEnteredInCall = true;
      room.enteredInCall = Boolean(
        room.callerEnteredInCall && room.calleeEnteredInCall
      );
    }
    room.lastHeartbeat = Date.now();

    if (typeof callback === "function") {
      callback({ exists: true, room });
    }

    socket.emit("room_snapshot", room);

    // Instant 0ms Replay of Buffered Offer / Answer / ICE Candidates
    if (emailKey === calleeKey && room.offer) {
      socket.emit("webrtc_offer", { roomId, offer: room.offer });
    }
    if (emailKey === calleeKey && room.callerCandidates.length > 0) {
      room.callerCandidates.forEach((candidate) => {
        socket.emit("webrtc_ice_candidate", {
          roomId,
          role: "caller",
          candidate,
        });
      });
    }
    if (emailKey === callerKey && room.answer) {
      socket.emit("webrtc_answer", { roomId, answer: room.answer });
    }
    if (emailKey === callerKey && room.calleeCandidates.length > 0) {
      room.calleeCandidates.forEach((candidate) => {
        socket.emit("webrtc_ice_candidate", {
          roomId,
          role: "callee",
          candidate,
        });
      });
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
        updates.status
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

  // Explicit Exit Events (End, Next, Background, Internet Lost) -> 0ms Teardown
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
      destroyRoomAndNotifyPartner(roomId, email, reason);
    }
  };

  socket.on("leave_room", handleExplicitExit("ended"));
  socket.on("videoend", handleExplicitExit("ended"));
  socket.on("videonext", handleExplicitExit("next"));
  socket.on("videobackground", handleExplicitExit("background"));
  socket.on("checkInternet", handleExplicitExit("disconnected"));

  // 7. SOCKET DISCONNECT (0ms Queue Removal + Instant Hold Drop Detection)
  socket.on("disconnect", (disconnectReason) => {
    const emailKey = socketToUser.get(socket.id);
    if (!emailKey) return;

    socketToUser.delete(socket.id);
    if (userToSocket.get(emailKey) !== socket.id) {
      return;
    }

    userToSocket.delete(emailKey);

    // 1. Always remove from waitingQueue at 0ms when socket disconnects
    removeFromWaitingQueue(emailKey, `socket_disconnect:${disconnectReason}`);

    const activeRoomId = userToRoom.get(emailKey);
    if (!activeRoomId || !activeRooms.has(activeRoomId)) return;

    const room = activeRooms.get(activeRoomId);
    const isProtected = (userProtectedUntil.get(emailKey) || 0) > Date.now();

    // 2. If user disconnected during the 2.5s RandomConnectingScreen hold (before entering InCall),
    // destroy the room immediately (400ms) so the partner is NOT pushed into an empty InCallScreen!
    const graceMs = isProtected
      ? PROTECTED_AD_GRACE_MS
      : room.enteredInCall
      ? CALL_NETWORK_SWITCH_GRACE_MS
      : 400;

    clearDisconnectGraceTimer(emailKey);
    const timer = setTimeout(() => {
      disconnectGraceTimers.delete(emailKey);
      const currentSocketId = userToSocket.get(emailKey);
      const currentSocket = currentSocketId
        ? io.sockets.sockets.get(currentSocketId)
        : null;

      if (!currentSocket || !currentSocket.connected) {
        logEvent(
          "DISCONNECT_ROOM_CLOSE",
          `💀 ${emailKey} disconnected (${disconnectReason}). Closing room ${activeRoomId}`
        );
        destroyRoomAndNotifyPartner(activeRoomId, emailKey, "disconnected");
      }
    }, graceMs);

    disconnectGraceTimers.set(emailKey, timer);
  });
});

// ============================================================================
// ⏱️ 1-SECOND REAL-TIME QUEUE & ROOM HEALTH SWEEPER
// Automatically removes users from waitingQueue if 10s expires or data/net drops
// ============================================================================
setInterval(() => {
  const now = Date.now();
  for (const [emailKey, entry] of waitingQueue.entries()) {
    const sock = io.sockets.sockets.get(entry.socketId);
    const isSocketDead = !sock || !sock.connected;
    const isPingExpired = now - entry.lastPing > STALE_QUEUE_PING_MS;
    const isTimeExpired = now - entry.joinedAt >= QUEUE_MAX_WAIT_MS;

    if (isSocketDead || isPingExpired || isTimeExpired) {
      const reason = isSocketDead
        ? "socket_dead"
        : isPingExpired
        ? "data_or_network_lost"
        : "10s_timeout";
      removeFromWaitingQueue(emailKey, reason);
      if (isTimeExpired && sock && sock.connected) {
        sock.emit("queue_expired_10s", {
          email: entry.email,
          timestamp: now,
        });
      }
    }
  }

  // Clean up any room where one user never entered InCall within 6s of matching
  for (const [roomId, room] of activeRooms.entries()) {
    if (!room.enteredInCall && now - room.createdAtMs > 6500) {
      logEvent(
        "HOLD_TIMEOUT_CLEANUP",
        `Closing room ${roomId} because one user did not enter InCall`
      );
      destroyRoomAndNotifyPartner(roomId, room.caller, "left");
    } else if (now - (room.lastHeartbeat || now) > 90000) {
      destroyRoomAndNotifyPartner(roomId, room.caller, "disconnected");
    }
  }
}, 1000);

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