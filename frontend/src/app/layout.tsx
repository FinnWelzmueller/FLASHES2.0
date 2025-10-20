import type { ReactNode } from 'react'

export const metadata = {
  title: 'FLASHES2.0',
  description: 'The Flexible Alert System for High-Energy Sources',
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  )
}