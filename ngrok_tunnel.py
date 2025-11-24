"""
Ngrok Integration for External Access
"""
from pyngrok import ngrok
from config import NGROK_AUTH_TOKEN, NGROK_PORT
import time

class NgrokTunnel:
    def __init__(self):
        self.tunnel = None
        self.public_url = None
        
    def start_tunnel(self, port=NGROK_PORT):
        """Start ngrok tunnel"""
        try:
            # Set auth token
            ngrok.set_auth_token(NGROK_AUTH_TOKEN)
            
            # Start tunnel
            self.tunnel = ngrok.connect(port, "http")
            self.public_url = self.tunnel.public_url
            
            print(f"\n{'='*60}")
            print(f"🌐 Ngrok Tunnel Started Successfully!")
            print(f"{'='*60}")
            print(f"Local URL:  http://localhost:{port}")
            print(f"Public URL: {self.public_url}")
            print(f"{'='*60}\n")
            
            return self.public_url
            
        except Exception as e:
            print(f"Error starting ngrok tunnel: {e}")
            print("Make sure ngrok is installed and the auth token is correct")
            return None
    
    def stop_tunnel(self):
        """Stop ngrok tunnel"""
        if self.tunnel:
            ngrok.disconnect(self.tunnel.public_url)
            print("Ngrok tunnel stopped")
    
    def get_public_url(self):
        """Get the public URL"""
        return self.public_url
    
    def get_tunnels(self):
        """Get all active tunnels"""
        tunnels = ngrok.get_tunnels()
        return tunnels

if __name__ == "__main__":
    tunnel = NgrokTunnel()
    url = tunnel.start_tunnel()
    
    if url:
        print("Tunnel is active. Press Ctrl+C to stop...")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            tunnel.stop_tunnel()
            print("Tunnel stopped")
