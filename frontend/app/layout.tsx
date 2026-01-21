import type { Metadata } from "next";
import { Inter, Playfair_Display } from "next/font/google";
import "./globals.css";

const inter = Inter({
  variable: "--font-sans",
  subsets: ["latin"],
});

const playfair = Playfair_Display({
  variable: "--font-serif",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "AadharPulse",
  description: "Next-Gen Operational Intelligence Platform",
};

import { Sidebar } from "@/components/organisms/Sidebar";
import QueryProvider from "@/components/providers/query-provider";

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className="dark">
      <body
        className={`${inter.variable} ${playfair.variable} antialiased bg-background text-foreground overflow-hidden`}
      >
        <QueryProvider>
          <div className="flex h-screen w-full">
            <Sidebar />
            <main className="flex-1 overflow-y-auto bg-background">
              {children}
            </main>
          </div>
        </QueryProvider>
      </body>
    </html>
  );
}
