"use client"

import { MetricCard } from "@/components/molecules/MetricCard"
import { AlertCard } from "@/components/molecules/AlertCard"
import { WatchlistTable } from "@/components/organisms/WatchlistTable"
import { MaturityChart } from "@/components/organisms/MaturityChart"
import { Header } from "@/components/organisms/Header"
import { LayoutDashboard, Lock, History, Server, Activity } from "lucide-react"
import { useSynthesis } from "@/hooks/use-synthesis"
import { Skeleton } from "@/components/ui/skeleton"

export default function Home() {
  const { data, isLoading } = useSynthesis()

  if (isLoading) {
    return (
      <div className="p-8 space-y-8 min-h-screen bg-transparent">
        <Header />
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          <Skeleton className="h-32 w-full bg-white/5" />
          <Skeleton className="h-32 w-full bg-white/5" />
          <Skeleton className="h-32 w-full bg-white/5" />
          <Skeleton className="h-32 w-full bg-white/5" />
        </div>
      </div>
    )
  }

  // Calculate percentages or defaults
  const total = data?.total_districts || 0
  const mature = data?.cluster_distribution?.Mature || 0
  const emerging = data?.cluster_distribution?.Emerging || 0
  const churn = data?.cluster_distribution?.["High Churn"] || 0

  const saturation = total > 0 ? ((mature / total) * 100).toFixed(1) + "%" : "0%"
  const churnRate = total > 0 ? ((churn / total) * 100).toFixed(1) + "%" : "0%"

  return (
    <div className="p-8 space-y-8 min-h-screen bg-transparent">
      <Header />

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <MetricCard
          title="Market Maturity"
          value={saturation}
          icon={LayoutDashboard}
          delta={{ value: `+${emerging} New`, positive: true }}
          progressColor="success"
        />
        <MetricCard
          title="High Churn Districts"
          value={churn.toString()}
          icon={Activity}
          delta={{ value: churnRate, positive: false }}
          progressColor="danger"
        />
        <MetricCard
          title="Ghost Districts"
          value={data?.ghost_districts?.length.toString() || "0"}
          icon={History}
          delta={{ value: "Critical", neutral: true }}
          progressColor="warning"
        />
        <MetricCard
          title="System Status"
          value="Active"
          icon={Server}
          delta={{ value: "Stable", positive: true }}
          progressColor="success"
        />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 h-[400px]">
        <div className="lg:col-span-2 h-full">
          <MaturityChart />
        </div>
        <div className="lg:col-span-1 h-full">
          <AlertCard />
        </div>
      </div>

      <div className="pt-4">
        <WatchlistTable />
      </div>
    </div>
  )
}
