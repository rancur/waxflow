/** @type {import('next').NextConfig} */
const nextConfig = {
  output: 'standalone',
  async rewrites() {
    return [
      {
        source: '/api/:path*',
        // Server-side rewrite: use Docker service name for container-to-container communication
        destination: `${process.env.INTERNAL_API_URL || 'http://sync-api:8402'}/api/:path*`,
      },
    ]
  },
}
module.exports = nextConfig
