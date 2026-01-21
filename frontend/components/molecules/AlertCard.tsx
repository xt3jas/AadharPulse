import { AlertTriangle, MoreHorizontal } from "lucide-react"
import { Card, CardContent, CardHeader } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"

export function AlertCard() {
    return (
        <Card className="bg-gradient-to-br from-[#2D0A0A] to-[#1A0505] border-red-900/30 text-white relative overflow-hidden">
            <CardHeader className="flex flex-row items-center justify-between pb-2">
                <Badge variant="destructive" className="bg-red-900/50 text-red-400 border-red-800 hover:bg-red-900/60 uppercase text-[10px] tracking-wider px-2 py-0.5 rounded-sm">
                    Critical Priority
                </Badge>
                <Button variant="ghost" size="icon" className="h-6 w-6 text-red-400 hover:text-red-300 hover:bg-red-950/50">
                    <MoreHorizontal className="h-4 w-4" />
                </Button>
            </CardHeader>
            <CardContent className="space-y-4">
                <div className="h-10 w-10 rounded-md bg-red-950/50 border border-red-900 flex items-center justify-center">
                    <AlertTriangle className="h-5 w-5 text-red-500" />
                </div>

                <div>
                    <h3 className="text-lg font-serif font-bold tracking-tight mb-2">Biometric Sync Latency</h3>
                    <p className="text-sm text-red-200/70 leading-relaxed">
                        Northern Sector nodes reporting &gt;400ms delay. Immediate infrastructure audit required to prevent cascade failure.
                    </p>
                </div>

                <div className="flex gap-3 pt-2">
                    <Button variant="outline" className="h-8 bg-white text-black hover:bg-gray-100 border-0 text-xs font-medium w-full">
                        View Directive
                    </Button>
                    <Button variant="outline" className="h-8 bg-transparent text-red-400 border-red-900/50 hover:bg-red-950 hover:text-red-300 text-xs font-medium w-auto">
                        Dismiss
                    </Button>
                </div>
            </CardContent>
        </Card>
    )
}
