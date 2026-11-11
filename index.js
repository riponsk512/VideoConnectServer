// index.js (Place in the root folder of VideoConnectServer)
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
const STALE_QUEUE_PING_MS = 6500; // Evict ghost queue users after 6.5s without ping
const CALL_RECONNECT_GRACE_MS = 12000; // 12s grace window for Wi-Fi <-> Cellular switches during active calls
const CONNECTING_DISCONNECT_GRACE_MS = 2500; // 2.5s grace window during the 2s hold screen

// ============================================================================
// 🗂️ IN-MEMORY STORES (ZERO FIRESTORE READS / WRITES)
// ============================================================================
const waitingQueue = new Map(); // emailKey -> queueEntry
const activeRooms = new Map(); // roomId (20-char) -> roomObject
const userToSocket = new Map(); // emailKey -> socket.id
const socketToUser = new Map(); // socket.id -> emailKey
const userToRoom = new Map(); // emailKey -> roomId
const matchingLocks = new Set(); // emailKey (prevents duplicate simultaneous matches)
const queueTimers = new Map(); // emailKey -> Timeout (10s no-match timer)
const disconnectGraceTimers = new Map(); // emailKey -> Timeout (network switch grace timer)

// 🔑 Generates exact 20-character Firestore-style alphanumeric Room ID (e.g. "EZ2oZefe4ThVKjkjEjHo")
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

function removeFromWaitingQueue(emailKey) {
  if (!emailKey) return;
  clearQueueTimer(emailKey);
  waitingQueue.delete(emailKey);
  matchingLocks.delete(emailKey);
}

// ============================================================================
// 🔍 BIDIRECTIONAL FILTER & DAILY LIMIT COMPATIBILITY CHECK
// ============================================================================
function areUsersCompatible(userA, userB) {
  const genderA = userA.gender ? String(userA.gender).toLowerCase() : null;
  const genderB = userB.gender ? String(userB.gender).toLowerCase() : null;
  const limitA = userA.dailyMatchLimit || 3;
  const limitB = userB.dailyMatchLimit || 3;

  // 1. Daily non-VIP gender limit check for User A
  if (!userA.isVip) {
    if (
      genderA === "male" &&
      genderB === "female" &&
      (userA.femaleConnectCount || 0) >= limitA
    ) {
      return false;
    }
    if (
      genderA === "female" &&
      genderB === "male" &&
      (userA.maleConnectCount || 0) >= limitA
    ) {
      return false;
    }
  }

  // 2. Daily non-VIP gender limit check for User B
  if (!userB.isVip) {
    if (
      genderB === "male" &&
      genderA === "female" &&
      (userB.femaleConnectCount || 0) >= limitB
    ) {
      return false;
    }
    if (
      genderB === "female" &&
      genderA === "male" &&
      (userB.maleConnectCount || 0) >= limitB
    ) {
      return false;
    }
  }

  // 3. Does User A match User B's filters?
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

  if (!aMatchesB) return false;

  // 4. Does User B match User A's filters?
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

  return bMatchesA;
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

  if (!partnerSocketId) return;

  const isStillInConnectingHold =
    forceConnectingRequeue || !room.enteredInCall;

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
  // 1. REGISTER USER (Cancels any pending network-switch grace timer on reconnect)
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
  });

  // 2. JOIN MATCHMAKING QUEUE (0ms In-Memory Pairing, Anti-Duplicate Lock)
  socket.on("queue_join", (payload) => {
    if (!payload?.email) return;
    const myEmail = String(payload.email).trim();
    const myKey = myEmail.toLowerCase();

    clearDisconnectGraceTimer(myKey);
    userToSocket.set(myKey, socket.id);
    socketToUser.set(socket.id, myKey);

    // Clean up any previous room before starting a fresh search
    const oldRoomId = userToRoom.get(myKey);
    if (oldRoomId) {
      destroyRoomAndNotifyPartner(oldRoomId, myEmail, "left", true);
    }

    if (matchingLocks.has(myKey)) return;
    matchingLocks.add(myKey);

    const now = Date.now();
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

    let matchedPartner = null;

    for (const [candKey, candidate] of waitingQueue.entries()) {
      if (candKey === myKey) continue;
      if (matchingLocks.has(candKey)) continue;
      if (userToRoom.has(candKey)) {
        removeFromWaitingQueue(candKey);
        continue;
      }

      const candSocket = io.sockets.sockets.get(candidate.socketId);
      if (
        !candSocket ||
        !candSocket.connected ||
        now - candidate.lastPing > STALE_QUEUE_PING_MS
      ) {
        removeFromWaitingQueue(candKey);
        continue;
      }

      if (areUsersCompatible(myEntry, candidate)) {
        matchingLocks.add(candKey);
        matchedPartner = candidate;
        removeFromWaitingQueue(candKey);
        break;
      }
    }

    // 🎉 MATCH FOUND: Create 20-Char Room in RAM & Emit to Both Users Simultaneously
    if (matchedPartner) {
      const partnerKey = matchedPartner.email.toLowerCase();
      removeFromWaitingQueue(myKey);
      removeFromWaitingQueue(partnerKey);

      const roomId = generateRoomId(20); // e.g. "EZ2oZefe4ThVKjkjEjHo"

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
        enteredInCall: false,
        status: "active",
      };

      activeRooms.set(roomId, roomObject);
      userToRoom.set(partnerKey, roomId);
      userToRoom.set(myKey, roomId);

      const callerSocket = io.sockets.sockets.get(matchedPartner.socketId);
      if (callerSocket) callerSocket.join(roomId);
      socket.join(roomId);

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

    // Place user in waiting queue with 10s timeout
    waitingQueue.set(myKey, myEntry);
    matchingLocks.delete(myKey);

    clearQueueTimer(myKey);
    const expiryTimer = setTimeout(() => {
      if (waitingQueue.has(myKey) && !userToRoom.has(myKey)) {
        removeFromWaitingQueue(myKey);
        socket.emit("queue_expired_10s", {
          email: myEmail,
          timestamp: Date.now(),
        });
      }
    }, QUEUE_MAX_WAIT_MS);
    queueTimers.set(myKey, expiryTimer);
  });

  // 3. 2-SECOND QUEUE PING
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

  // 4. LEAVE QUEUE / INSTANT ABORT
  socket.on("queue_leave", (payload) => {
    const email = payload?.email || socketToUser.get(socket.id);
    if (!email) return;
    const key = String(email).trim().toLowerCase();
    removeFromWaitingQueue(key);

    if (payload?.explicitAbort) {
      const roomId = payload.roomId || userToRoom.get(key);
      if (roomId) {
        destroyRoomAndNotifyPartner(
          roomId,
          email,
          payload.dropStatus || payload.reason || "left",
          true
        );
      }
    }
  });

  // 5. MARK TRANSITION FROM 2s HOLD TO INCALL SCREEN
  socket.on("entered_incall", (payload) => {
    const { roomId, email } = payload || {};
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
    }
  });

  // ==========================================================================
  // 🎥 INCALL WEBRTC SIGNALING & STATE SYNC EVENTS
  // ==========================================================================

  // Join Room & Return Current Room State Immediately
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
    } else {
      socket.emit("room_ended", { roomId, status: "deleted" });
    }
  });

  // Update Room Fields (callerMuted, calleeMuted, callerQuality, calleeQuality, Filters, Privacy, Gifts)
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

  // WebRTC Offer
  socket.on("webrtc_offer", (payload) => {
    const { roomId, offer } = payload || {};
    if (!roomId || !offer || !activeRooms.has(roomId)) return;
    const room = activeRooms.get(roomId);
    room.offer = offer;
    room.lastHeartbeat = Date.now();
    socket.to(roomId).emit("webrtc_offer", { roomId, offer });
    io.to(roomId).emit("room_snapshot", room);
  });

  // WebRTC Answer
  socket.on("webrtc_answer", (payload) => {
    const { roomId, answer } = payload || {};
    if (!roomId || !answer || !activeRooms.has(roomId)) return;
    const room = activeRooms.get(roomId);
    room.answer = answer;
    room.lastHeartbeat = Date.now();
    socket.to(roomId).emit("webrtc_answer", { roomId, answer });
    io.to(roomId).emit("room_snapshot", room);
  });

  // WebRTC ICE Candidate (callerCandidates & calleeCandidates)
  socket.on("webrtc_ice_candidate", (payload) => {
    const { roomId, role, candidate } = payload || {};
    if (!roomId || !candidate || !activeRooms.has(roomId)) return;
    const room = activeRooms.get(roomId);

    if (String(role).toLowerCase() === "caller") {
      room.callerCandidates.push(candidate);
    } else {
      room.calleeCandidates.push(candidate);
    }
    room.lastHeartbeat = Date.now();

    socket.to(roomId).emit("webrtc_ice_candidate", {
      roomId,
      role,
      candidate,
    });
  });

  // Seamless ICE Restart Request / Offer / Answer
  socket.on("webrtc_ice_restart", (payload) => {
    const { roomId, iceRestartRequest, iceRestartOffer, iceRestartAnswer } =
      payload || {};
    if (!roomId || !activeRooms.has(roomId)) return;
    const room = activeRooms.get(roomId);

    if (iceRestartRequest !== undefined)
      room.iceRestartRequest = iceRestartRequest;
    if (iceRestartOffer !== undefined) room.iceRestartOffer = iceRestartOffer;
    if (iceRestartAnswer !== undefined)
      room.iceRestartAnswer = iceRestartAnswer;
    room.lastHeartbeat = Date.now();

    socket.to(roomId).emit("webrtc_ice_restart", payload);
    io.to(roomId).emit("room_snapshot", room);
  });

  // Room Heartbeat
  socket.on("room_heartbeat", (payload) => {
    const { roomId } = payload || {};
    if (!roomId || !activeRooms.has(roomId)) return;
    const room = activeRooms.get(roomId);
    room.lastHeartbeat = Date.now();
  });

  // Explicit Exit Events (0ms Immediate Teardown)
  const handleExplicitExit = (defaultReason) => (payload) => {
    const email =
      (typeof payload === "object" && payload?.email) ||
      socketToUser.get(socket.id);
    const emailKey = email ? String(email).toLowerCase() : null;
    if (emailKey) removeFromWaitingQueue(emailKey);

    const roomId =
      typeof payload === "string"
        ? payload
        : payload?.roomId || (emailKey ? userToRoom.get(emailKey) : null);

    if (roomId) {
      const reason =
        (typeof payload === "object" && (payload?.reason || payload?.status)) ||
        defaultReason;
      destroyRoomAndNotifyPartner(roomId, email, reason, false);
    }
  };

  socket.on("leave_room", handleExplicitExit("ended"));
  socket.on("videoend", handleExplicitExit("ended"));
  socket.on("videonext", handleExplicitExit("next"));
  socket.on("videobackground", handleExplicitExit("background"));
  socket.on("checkInternet", handleExplicitExit("disconnected"));

  // 6. SMART DISCONNECT HANDLER (Protects Wi-Fi <-> Cellular Switches During Active Calls)
  socket.on("disconnect", () => {
    const emailKey = socketToUser.get(socket.id);
    if (!emailKey) return;

    socketToUser.delete(socket.id);
    if (userToSocket.get(emailKey) === socket.id) {
      userToSocket.delete(emailKey);
    }

    // Always remove immediately from waiting queue if they were still searching
    removeFromWaitingQueue(emailKey);

    const activeRoomId = userToRoom.get(emailKey);
    if (!activeRoomId || !activeRooms.has(activeRoomId)) return;

    const room = activeRooms.get(activeRoomId);
    const graceMs = room.enteredInCall
      ? CALL_RECONNECT_GRACE_MS
      : CONNECTING_DISCONNECT_GRACE_MS;

    clearDisconnectGraceTimer(emailKey);
    const timer = setTimeout(() => {
      disconnectGraceTimers.delete(emailKey);
      // If user has not reconnected with a new socket within the grace window, destroy room
      const currentSocketId = userToSocket.get(emailKey);
      const currentSocket = currentSocketId
        ? io.sockets.sockets.get(currentSocketId)
        : null;
      if (!currentSocket || !currentSocket.connected) {
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

// Background garbage collector for abandoned rooms (runs every 60s)
setInterval(() => {
  const now = Date.now();
  for (const [roomId, room] of activeRooms.entries()) {
    if (now - (room.lastHeartbeat || now) > 120000) {
      destroyRoomAndNotifyPartner(roomId, room.caller, "disconnected", false);
    }
  }
}, 60000);

// Health Check Endpoints for Render
app.get("/", (req, res) => {
  res.status(200).json({
    server: "VideoConnectServer",
    status: "online",
    waitingInQueue: waitingQueue.size,
    activeRooms: activeRooms.size,
    uptimeSeconds: Math.floor(process.uptime()),
  });
});

app.get("/health", (req, res) => {
  res.status(200).send("OK");
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`🚀 VideoConnectServer running on 0.0.0.0:${PORT}`);
});