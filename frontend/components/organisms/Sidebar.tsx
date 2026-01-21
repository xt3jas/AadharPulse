"use client"

import Link from "next/link"
import { usePathname } from "next/navigation"
import { LayoutGrid, Map, FileText, BarChart3, Settings, Eye } from "lucide-react"
import { cn } from "@/lib/utils"
import { Avatar, AvatarFallback, AvatarImage } from "@/components/ui/avatar"
import { Badge } from "@/components/ui/badge"

const navItems = [
    {
        title: "Overview",
        href: "/",
        icon: LayoutGrid,
    },
    {
        title: "Regional Data",
        href: "/regional",
        icon: Map,
    },
    {
        title: "Directives",
        href: "/directives",
        icon: FileText,
        badge: 3,
    },
    {
        title: "Analytics",
        href: "/analytics",
        icon: BarChart3,
    },
    {
        title: "Surveillance",
        href: "/surveillance",
        icon: Eye,
    }
]

export function Sidebar() {
    const pathname = usePathname()

    return (
        <div className="flex h-screen w-64 flex-col border-r bg-sidebar text-sidebar-foreground">
            {/* Header */}
            <div className="p-6">
                <h1 className="font-serif text-2xl font-bold tracking-tight">AadharPulse</h1>
                <p className="text-xs font-medium text-muted-foreground tracking-widest text-[10px] mt-1">GOV INTELLIGENCE</p>
            </div>

            {/* Navigation */}
            <div className="flex-1 px-4 py-4 space-y-1">
                {navItems.map((item) => {
                    const isActive = pathname === item.href
                    return (
                        <Link
                            key={item.href}
                            href={item.href}
                            className={cn(
                                "flex items-center gap-3 rounded-md px-3 py-2.5 text-sm font-medium transition-colors hover:bg-sidebar-accent hover:text-sidebar-accent-foreground",
                                isActive ? "bg-primary text-primary-foreground hover:bg-primary/90 hover:text-primary-foreground" : "text-muted-foreground"
                            )}
                        >
                            <item.icon className="h-4 w-4" />
                            <span className="flex-1">{item.title}</span>
                            {item.badge && (
                                <Badge variant={isActive ? "secondary" : "default"} className={cn("ml-auto bg-primary text-primary-foreground h-5 px-1.5", isActive && "bg-primary-foreground text-primary")}>
                                    {item.badge}
                                </Badge>
                            )}
                        </Link>
                    )
                })}
            </div>

            {/* Footer / Settings */}
            <div className="p-4 border-t border-sidebar-border mt-auto">
                <Link
                    href="/settings"
                    className={cn(
                        "flex items-center gap-3 rounded-md px-3 py-2.5 text-sm font-medium transition-colors hover:bg-sidebar-accent hover:text-sidebar-accent-foreground text-muted-foreground mb-4"
                    )}
                >
                    <Settings className="h-4 w-4" />
                    <span>Settings</span>
                </Link>

                <div className="flex items-center gap-3 rounded-xl bg-card p-3 border border-sidebar-border">
                    <Avatar className="h-9 w-9 bg-primary/20 text-primary">
                        <AvatarFallback>JD</AvatarFallback>
                    </Avatar>
                    <div className="flex flex-col overflow-hidden">
                        <span className="text-sm font-medium truncate">J. Doe</span>
                        <span className="text-xs text-muted-foreground truncate">Admin • Level 4</span>
                    </div>
                </div>
            </div>
        </div>
    )
}
