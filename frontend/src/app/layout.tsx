import type { Metadata } from "next";
import "./globals.css";
import Link from "next/link";
import { Space_Grotesk, JetBrains_Mono } from "next/font/google";

const grotesk = Space_Grotesk({
  subsets: ["latin"],
  variable: "--font-sans",
  display: "swap",
});
const mono = JetBrains_Mono({
  subsets: ["latin"],
  variable: "--font-mono",
  display: "swap",
});

export const metadata: Metadata = {
  title: "FLASHES2.0",
  description: "The Flexible Alert System for High-Energy Sources",
  metadataBase: new URL("https://flashes.local"),
  themeColor: "#0a0a0a",
  openGraph: {
    title: "FLASHES2.0",
    description: "The Flexible Alert System for High-Energy Sources",
    type: "website",
  },
  icons: {
    icon: "/favicon.ico",
  },
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" suppressHydrationWarning className={`${grotesk.variable} ${mono.variable}`}>
      <body className="min-h-dvh bg-neutral-950 text-neutral-100 antialiased">
        {/* Header */}
        <header className="sticky top-0 z-50 backdrop-blur bg-neutral-950/70 border-b border-neutral-900">
          <div className="mx-auto max-w-6xl px-4 py-3 flex items-center justify-between">
            <Link href="/" className="font-semibold tracking-wide">
              FLASHES<span className="text-neutral-400">2.0</span> <span className="text-red-400">&beta;</span>
            </Link>
            <nav className="flex items-center gap-5 text-sm">
              <Link href="/sources" className="hover:underline">Sources</Link>
              <Link href="/tags" className="hover:underline">Tags</Link>
              <Link href="/about" className="hover:underline">About</Link>
            </nav>
          </div>
        </header>

        {/* Main */}
        <main className="flex-1 mx-auto max-w-6xl px-4 py-6">{children}</main>

        {/* Footer */}
         <footer className="border-t border-neutral-900">
            <div className="mx-auto max-w-6xl px-4 py-3 text-neutral-400 text-xs">
            © {new Date().getFullYear()} FLASHES2.0  Built with Next.js
          </div>
        </footer>
      </body>
    </html>
  );
}
