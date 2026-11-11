// src/confiq/VideoConnectSocketManager.js
import { io } from "socket.io-client";
import { auth } from "./firebase";

// 🌐 PUT YOUR LIVE RENDER URL HERE (No trailing slash)
export const VIDEO_CONNECT_SERVER_URL = "https://videoconnectserver.onrender.com";

class VideoConnectSocketManager {
  constructor() {
    this.currentScreen = null;
    this.currentRoomId = null;
    this.inQueue = false;
    this.isUserActive = true;
    this.isAdOpen = false;
    this.userEmail = null;
    this.userUid = null;

    this.wakeUpServer();

    this.socket = io(VIDEO_CONNECT_SERVER_URL, {
      transports: ["websocket", "polling"],
      autoConnect: true,
      reconnection: true,
      reconnectionAttempts: Infinity,
      reconnectionDelay: 400,
      reconnectionDelayMax: 2000,
      timeout: 8000,
    });

    this.io = this.socket;
    this.client = this.socket;

    this.setupSocketLifecycle();
  }

  wakeUpServer() {
    try {
      fetch(`${VIDEO_CONNECT_SERVER_URL}/health`).catch(() => {});
    } catch (e) {}
  }

  setupSocketLifecycle() {
    this.socket.on("connect", () => {
      const email = auth.currentUser?.email || this.userEmail;
      const uid = auth.currentUser?.uid || this.userUid;

      if (email) {
        this.userEmail = email;
        this.userUid = uid;
        this.socket.emit("register_user", {
          email,
          uid,
          screen: this.currentScreen,
          roomId: this.currentRoomId,
          timestamp: Date.now(),
        });
      }

      if (this.isAdOpen && email) {
        this.socket.emit("ad_status", {
          email,
          roomId: this.currentRoomId,
          isActive: true,
        });
      }

      // Rejoin active call room if socket reconnects mid-call
      if (this.currentRoomId && !this.inQueue && email) {
        this.socket.emit("join_room", {
          roomId: this.currentRoomId,
          email,
        });
      }
    });

    this.socket.on("connect_error", (err) => {
      console.log(
        "⚠️ [VideoConnectSocketManager] Connection error:",
        err?.message
      );
    });
  }

  emit(eventName, payload, callback) {
    try {
      if (!this.socket.connected) {
        this.socket.connect();
      }
      if (typeof callback === "function") {
        this.socket.emit(eventName, payload, callback);
      } else {
        this.socket.emit(eventName, payload);
      }
    } catch (e) {}
  }

  setScreen(screenName) {
    this.currentScreen = screenName;
  }

  setUserActiveLocally(isActive) {
    this.isUserActive = Boolean(isActive);
  }

  setInQueue(inQueueStatus, email = null) {
    this.inQueue = Boolean(inQueueStatus);
    if (inQueueStatus) {
      this.currentRoomId = null;
    }
    if (email) this.userEmail = email;
  }

  setInRoom(roomId) {
    this.currentRoomId = roomId || null;
    if (roomId) {
      this.inQueue = false;
    }
  }

  setAdStatus(isAdActive, roomId = null) {
    this.isAdOpen = Boolean(isAdActive);
    if (roomId) this.currentRoomId = roomId;
    const email = auth.currentUser?.email || this.userEmail;
    if (email) {
      this.emit("ad_status", {
        email,
        roomId: this.currentRoomId,
        isActive: Boolean(isAdActive),
      });
    }
  }

  roomEstablished(roomId, myEmail, remoteEmail) {
    this.currentRoomId = roomId;
    this.inQueue = false;
    if (myEmail) this.userEmail = myEmail;
    this.emit("join_room", {
      roomId,
      email: myEmail || auth.currentUser?.email,
      partnerEmail: remoteEmail,
    });
  }

  videoend(roomId) {
    const targetRoom = roomId || this.currentRoomId;
    if (!targetRoom) return;
    this.emit("videoend", {
      roomId: targetRoom,
      email: auth.currentUser?.email || this.userEmail,
      reason: "ended",
      timestamp: Date.now(),
    });
    this.currentRoomId = null;
  }

  videonext(roomId) {
    const targetRoom = roomId || this.currentRoomId;
    if (!targetRoom) return;
    this.emit("videonext", {
      roomId: targetRoom,
      email: auth.currentUser?.email || this.userEmail,
      reason: "next",
      timestamp: Date.now(),
    });
    this.currentRoomId = null;
  }

  videobackground(roomId) {
    const targetRoom = roomId || this.currentRoomId;
    this.emit("videobackground", {
      roomId: targetRoom,
      email: auth.currentUser?.email || this.userEmail,
      reason: "background",
      timestamp: Date.now(),
    });
    this.currentRoomId = null;
  }

  checkInternet(roomId) {
    const targetRoom = roomId || this.currentRoomId;
    this.emit("checkInternet", {
      roomId: targetRoom,
      email: auth.currentUser?.email || this.userEmail,
      reason: "disconnected",
      timestamp: Date.now(),
    });
    this.currentRoomId = null;
  }
}

export const videoConnectSocket = new VideoConnectSocketManager();
export const socketManager = videoConnectSocket;
export default videoConnectSocket;