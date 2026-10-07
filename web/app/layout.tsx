import "./globals.css";
import Link from "next/link";

export const metadata = { title: "Vytals — Real-Time Vital Risk", description: "Kafka + Spark + XGBoost real-time patient monitoring" };

export default function Root({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className="min-h-screen">
        {/* Apple-style frosted nav */}
        <nav className="sticky top-0 z-50 border-b bg-white/70 backdrop-blur-2xl" style={{ borderColor: "rgba(0,0,0,0.08)" }}>
          <div className="mx-auto flex max-w-6xl items-center gap-4 px-6 py-3">
            <Link href="/" className="flex items-center gap-3">
              <div className="grid h-9 w-9 place-items-center rounded-[10px] bg-black font-bold text-white">
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="white" strokeWidth="2.4" strokeLinecap="round" strokeLinejoin="round"><path d="M22 12h-4l-3 9L9 3l-3 9H2" /></svg>
              </div>
              <div>
                <div className="apple-title text-[17px] leading-tight">Vytals</div>
                <div className="text-[12px] apple-secondary">Real-time vital risk</div>
              </div>
            </Link>
            <div className="ml-auto flex items-center gap-1 text-[15px]">
              <span className="pulse-dot mr-1 inline-block h-2 w-2 rounded-full bg-green-500" />
              <span className="apple-secondary mr-3 text-[13px]">Live</span>
              <Link href="/" className="rounded-full bg-black px-4 py-1.5 text-white">Dashboard</Link>
            </div>
          </div>
        </nav>
        <main className="mx-auto max-w-6xl px-6 py-6">{children}</main>
        <footer className="mx-auto max-w-6xl px-6 pb-10 text-[12px] apple-secondary">
          Vytals MVP • Risk Score 0–100 • Kafka vitals.raw (3 partitions) • 5-min window / 1-min slide • XGBoost
        </footer>
      </body>
    </html>
  );
}
