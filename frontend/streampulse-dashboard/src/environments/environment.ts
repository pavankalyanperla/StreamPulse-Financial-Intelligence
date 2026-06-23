export const environment = {
  production: false,
  gatewayUrl: '',           // empty → relative URLs, proxied to http://localhost:5000
  signalRUrl: '/hubs/stocks' // proxied via "ws":true in proxy.conf.json
};
