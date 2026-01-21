"use client"

import { ScatterChart, Scatter, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Label } from 'recharts';
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs"

const data = [
    { x: 100, y: 200, z: 200, fill: '#2563EB' }, // Blue
    { x: 120, y: 100, z: 260, fill: '#EF4444' }, // Red/Orange
    { x: 170, y: 300, z: 400, fill: '#22C55E' }, // Green
    { x: 140, y: 250, z: 280, fill: '#2563EB' },
    { x: 150, y: 400, z: 500, fill: '#22C55E' },
    { x: 110, y: 280, z: 200, fill: '#2563EB' },
    { x: 50, y: 30, z: 100, fill: '#F59E0B' }, // Warning
    { x: 180, y: 350, z: 300, fill: '#22C55E' },
];

export function MaturityChart() {
    return (
        <Card className="bg-card border-none shadow-none h-full">
            <CardHeader className="flex flex-row items-center justify-between pb-2">
                <div>
                    <CardTitle className="text-lg font-serif font-medium">National Maturity Index</CardTitle>
                    <p className="text-sm text-muted-foreground">Operational Efficiency vs. Digital Adoption</p>
                </div>
                <Tabs defaultValue="q1" className="h-8">
                    <TabsList className="h-8 bg-muted/50 border border-white/5">
                        <TabsTrigger value="q1" className="text-xs h-6">Q1</TabsTrigger>
                        <TabsTrigger value="q2" className="text-xs h-6">Q2</TabsTrigger>
                        <TabsTrigger value="q3" className="text-xs h-6">Q3</TabsTrigger>
                    </TabsList>
                </Tabs>
            </CardHeader>
            <CardContent className="h-[300px] w-full">
                <ResponsiveContainer width="100%" height="100%">
                    <ScatterChart margin={{ top: 20, right: 20, bottom: 20, left: 20 }}>
                        <CartesianGrid strokeDasharray="3 3" opacity={0.1} />
                        <XAxis type="number" dataKey="x" name="Digital Adoption" hide />
                        <YAxis type="number" dataKey="y" name="Efficiency" hide />
                        <Tooltip cursor={{ strokeDasharray: '3 3' }} contentStyle={{ backgroundColor: '#1A1A1E', borderColor: '#27272A', color: '#fff' }} />
                        <Scatter name="Districts" data={data} fill="#8884d8" />
                    </ScatterChart>
                </ResponsiveContainer>
                {/* Custom Axis Labels Overlay */}
                <div className="relative w-full h-0">
                    <span className="absolute -top-4 left-1/2 -translate-x-1/2 text-xs text-muted-foreground tracking-widest uppercase">Digital Adoption</span>
                    <span className="absolute -top-[150px] -left-8 -rotate-90 text-xs text-muted-foreground tracking-widest uppercase">Efficiency</span>
                </div>
            </CardContent>
        </Card>
    )
}
