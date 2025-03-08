from aiohttp import web
import json
import logging
from collections import deque

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('SERVER')

connections = {}
pending_peers = deque()

async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    peer_id = None

    try:
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                data = json.loads(msg.data)
                
                if data['type'] == 'login':
                    peer_id = data['peer_id']
                    connections[peer_id] = ws
                    logger.info(f"Peer {peer_id} connected")

                    if len(connections) == 1:
                        await ws.send_json({'type': 'wait'})
                        pending_peers.append(peer_id)
                        logger.info(f"Peer {peer_id} waiting for another peer")
                    elif len(connections) == 2:
                        initiator = pending_peers.popleft()
                        responder = peer_id
                        
                        await connections[initiator].send_json({
                            'type': 'start_call',
                            'peer_id': responder
                        })
                        await connections[responder].send_json({
                            'type': 'peer_connected',
                            'peer_id': initiator
                        })

                elif data['type'] in ['offer', 'answer', 'candidate']:
                    for p in connections:
                        if p != peer_id:
                            await connections[p].send_json(data)
                            break

    except Exception as e:
        logger.error(f"Error: {str(e)}")
    finally:
        if peer_id:
            connections.pop(peer_id, None)
            if peer_id in pending_peers:
                pending_peers.remove(peer_id)
            logger.info(f"Peer {peer_id} disconnected")
    return ws

app = web.Application()
app.add_routes([web.get('/ws', websocket_handler)])

if __name__ == '__main__':
    web.run_app(app, port=8080)

