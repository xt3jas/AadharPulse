"use client"

import { useState } from "react"
import { useMutation } from "@tanstack/react-query"
import { Upload, Check, AlertCircle, Loader2 } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Header } from "@/components/organisms/Header"
import { uploadFile } from "@/lib/api"

export default function SettingsPage() {
    const [file, setFile] = useState<File | null>(null)

    const uploadMutation = useMutation({
        mutationFn: uploadFile,
    })

    const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
        if (e.target.files && e.target.files[0]) {
            setFile(e.target.files[0])
            uploadMutation.reset()
        }
    }

    const handleUpload = () => {
        if (file) {
            uploadMutation.mutate(file)
        }
    }

    return (
        <div className="p-8 space-y-8 min-h-screen">
            <div className="flex items-end justify-between pb-6 pt-2">
                <div>
                    <p className="text-xs font-semibold tracking-widest text-muted-foreground uppercase mb-2">System Configuration</p>
                    <h2 className="text-3xl font-serif font-bold text-foreground">Settings & Data</h2>
                </div>
            </div>

            <div className="grid gap-6 max-w-2xl">
                <Card>
                    <CardHeader>
                        <CardTitle>Data Ingestion</CardTitle>
                        <CardDescription>
                            Upload raw Aadhar enrollment CSV logs for processing. The system will automatically ingest, validate, and update the analytics engine.
                        </CardDescription>
                    </CardHeader>
                    <CardContent className="space-y-4">
                        <div className="grid w-full items-center gap-1.5">
                            <Label htmlFor="csv_upload">Enrollment Data (CSV)</Label>
                            <div className="flex gap-4">
                                <Input
                                    id="csv_upload"
                                    type="file"
                                    accept=".csv"
                                    onChange={handleFileChange}
                                    className="cursor-pointer file:cursor-pointer"
                                />
                                <Button
                                    onClick={handleUpload}
                                    disabled={!file || uploadMutation.isPending}
                                    className="min-w-[120px]"
                                >
                                    {uploadMutation.isPending ? (
                                        <>
                                            <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                                            Uploading
                                        </>
                                    ) : (
                                        <>
                                            <Upload className="mr-2 h-4 w-4" />
                                            Upload
                                        </>
                                    )}
                                </Button>
                            </div>
                        </div>

                        {uploadMutation.isSuccess && (
                            <Alert className="bg-emerald-500/10 border-emerald-500/20 text-emerald-500">
                                <Check className="h-4 w-4" />
                                <AlertTitle>Success</AlertTitle>
                                <AlertDescription>
                                    Data uploaded successfully. Processing pipeline triggered.
                                </AlertDescription>
                            </Alert>
                        )}

                        {uploadMutation.isError && (
                            <Alert variant="destructive" className="bg-red-500/10 border-red-500/20 text-red-500">
                                <AlertCircle className="h-4 w-4" />
                                <AlertTitle>Error</AlertTitle>
                                <AlertDescription>
                                    {uploadMutation.error.message || "Failed to upload file."}
                                </AlertDescription>
                            </Alert>
                        )}
                    </CardContent>
                </Card>
            </div>
        </div>
    )
}
