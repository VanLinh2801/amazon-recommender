/** @type {import('next').NextConfig} */
const nextConfig = {
  typescript: {
    ignoreBuildErrors: true,
  },
  images: {
    unoptimized: true,
  },
  // Use webpack instead of Turbopack for compatibility
  experimental: {
    turbo: false,
  },
}

export default nextConfig
