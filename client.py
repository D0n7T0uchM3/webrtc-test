import asyncio
import json
import logging
import threading
from aiohttp import ClientSession
from aiortc import RTCPeerConnection, RTCSessionDescription, RTCIceCandidate, VideoStreamTrack
from av import VideoFrame
from mss import mss
import cv2
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('CLIENT')

class ScreenVideoTrack(VideoStreamTrack):
    def __init__(self):
        super().__init__()
        self.sct = mss()
        self.monitor = self.sct.monitors[1]

    async def recv(self):
        pts, time_base = await self.next_timestamp()
        img = np.array(self.sct.grab(self.monitor))
        img = cv2.cvtColor(img, cv2.COLOR_BGRA2BGR)
        img = cv2.resize(img, (2560, 1440))
        frame = VideoFrame.from_ndarray(img, format='bgr24')
        frame.pts = pts
        frame.time_base = time_base
        return frame

class VideoRenderer:
    def __init__(self):
        self.window_name = "Remote Screen"
        cv2.namedWindow(self.window_name, cv2.WINDOW_NORMAL)
        self.frame = None
        self.lock = threading.Lock()
        self.is_running = True

    def update_frame(self, frame):
        with self.lock:
            self.frame = frame

    async def render_loop(self):
        while self.is_running:
            await asyncio.sleep(0.01)  # ~30 FPS
            with self.lock:
                if self.frame is not None:
                    try:
                        img = self.frame.to_ndarray(format="bgr24")
                        cv2.imshow(self.window_name, img)
                        cv2.waitKey(1)
                    except Exception as e:
                        logger.error(f"Render error: {str(e)}")

async def main():
    peer_id = input("Your peer ID: ")
    
    pc = RTCPeerConnection()
    renderer = VideoRenderer()
    render_task = asyncio.create_task(renderer.render_loop())
    is_initiator = False

    @pc.on("icecandidate")
    async def on_icecandidate(candidate):
        if candidate:
            await ws.send_json({
                "type": "candidate",
                "candidate": {
                    "candidate": candidate.candidate,
                    "sdpMid": candidate.sdpMid,
                    "sdpMLineIndex": candidate.sdpMLineIndex
                }
            })

    @pc.on("track")
    async def on_track(track):
        logger.info(f"Received remote track: {track.kind}")
        if track.kind == "video":
            while True:
                try:
                    frame = await track.recv()
                    renderer.update_frame(frame)
                except Exception as e:
                    logger.error(f"Track error: {str(e)}")
                    break

    try:
        async with ClientSession() as session:
            async with session.ws_connect('http://192.168.1.102:8081/ws') as ws:
                await ws.send_json({'type': 'login', 'peer_id': peer_id})

                async def listen_for_messages():
                    async for msg in ws:
                        data = json.loads(msg.data)
                        
                        if data['type'] == 'start_call':
                            logger.info("Starting call...")
                            pc.addTrack(ScreenVideoTrack())
                            offer = await pc.createOffer()
                            await pc.setLocalDescription(offer)
                            await ws.send_json({
                                'type': 'offer',
                                'sdp': pc.localDescription.sdp
                            })
                        
                        elif data['type'] == 'offer':
                            await pc.setRemoteDescription(
                                RTCSessionDescription(sdp=data['sdp'], type='offer')
                            )
                            answer = await pc.createAnswer()
                            await pc.setLocalDescription(answer)
                            await ws.send_json({
                                'type': 'answer',
                                'sdp': pc.localDescription.sdp
                            })
                        
                        elif data['type'] == 'answer':
                            await pc.setRemoteDescription(
                                RTCSessionDescription(sdp=data['sdp'], type='answer')
                            )
                        
                        elif data['type'] == 'candidate':
                            await pc.addIceCandidate(
                                RTCIceCandidate(
                                    candidate=data['candidate']['candidate'],
                                    sdpMid=data['candidate']['sdpMid'],
                                    sdpMLineIndex=data['candidate']['sdpMLineIndex']
                                )
                            )

                await listen_for_messages()

    finally:
        renderer.is_running = False
        await render_task
        await pc.close()
        cv2.destroyAllWindows()

if __name__ == '__main__':
    asyncio.run(main())

