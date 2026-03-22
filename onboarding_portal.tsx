import React, { useMemo, useState } from "react";
import {
  CheckCircle2,
  ChevronRight,
  ClipboardList,
  FileBadge2,
  FileCheck2,
  FileText,
  Filter,
  FolderOpen,
  Search,
  ShieldCheck,
  Signature,
  Upload,
  UserCircle2,
  Clock3,
  AlertCircle,
  Eye,
  Download,
  Building2,
  CalendarDays,
  Sparkles,
} from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import { Progress } from "@/components/ui/progress";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import { Textarea } from "@/components/ui/textarea";
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";

const THEME = "#7c4095";

type TaskType = "document" | "profile" | "signature";
type TaskStatus = "pending" | "in_progress" | "completed";

type Task = {
  id: string;
  type: TaskType;
  status: TaskStatus;
  title: string;
  description: string;
  dueDate: string;
  requestedBy: string;
  sectionName?: string;
  fields?: { label: string; value?: string; required?: boolean; placeholder?: string }[];
  documentName?: string;
  formName?: string;
};

const tasks: Task[] = [
  {
    id: "DOC-1001",
    type: "document",
    status: "pending",
    title: "Upload DEA Certificate",
    description:
      "Please upload your current DEA certificate. Make sure the copy is clear and all dates are visible.",
    dueDate: "Mar 28, 2026",
    requestedBy: "Credentialing Admin",
    documentName: "DEA Certificate.pdf",
  },
  {
    id: "PRO-1002",
    type: "profile",
    status: "in_progress",
    title: "Complete License Information",
    description:
      "Fill in your active state license details and related information so the credentialing team can continue verification.",
    dueDate: "Mar 25, 2026",
    requestedBy: "Credentialing Admin",
    sectionName: "License Information",
    fields: [
      { label: "License Number", value: "TX-928188", required: true, placeholder: "Enter license number" },
      { label: "State", value: "Texas", required: true, placeholder: "Select state" },
      { label: "Issue Date", placeholder: "MM/DD/YYYY" },
      { label: "Expiration Date", required: true, placeholder: "MM/DD/YYYY" },
    ],
  },
  {
    id: "SIG-1003",
    type: "signature",
    status: "pending",
    title: "Sign Provider Enrollment Packet",
    description:
      "Review and sign the enrollment packet. Your signature is required before submission to the payer.",
    dueDate: "Mar 24, 2026",
    requestedBy: "Enrollment Team",
    formName: "Provider Enrollment Packet.pdf",
  },
  {
    id: "DOC-0901",
    type: "document",
    status: "completed",
    title: "Uploaded Board Certification",
    description: "Board certification document successfully submitted.",
    dueDate: "Completed on Mar 18, 2026",
    requestedBy: "Credentialing Admin",
    documentName: "Board Certification.pdf",
  },
  {
    id: "PRO-0902",
    type: "profile",
    status: "completed",
    title: "Completed DEA Information",
    description: "DEA profile section was completed and submitted.",
    dueDate: "Completed on Mar 17, 2026",
    requestedBy: "Credentialing Admin",
    sectionName: "DEA Information",
    fields: [
      { label: "DEA Number", value: "BR1234567" },
      { label: "State", value: "Texas" },
      { label: "Expiration Date", value: "11/30/2027" },
    ],
  },
  {
    id: "SIG-0903",
    type: "signature",
    status: "completed",
    title: "Signed W-9 Form",
    description: "W-9 form reviewed and signed.",
    dueDate: "Completed on Mar 15, 2026",
    requestedBy: "Operations Team",
    formName: "W-9 Form.pdf",
  },
];

const typeMeta: Record<
  TaskType,
  { label: string; icon: React.ElementType; chipClass: string; lightClass: string }
> = {
  document: {
    label: "Document Upload",
    icon: Upload,
    chipClass: "bg-blue-50 text-blue-700 border-blue-200",
    lightClass: "bg-blue-50",
  },
  profile: {
    label: "Profile Information",
    icon: ClipboardList,
    chipClass: "bg-violet-50 text-violet-700 border-violet-200",
    lightClass: "bg-violet-50",
  },
  signature: {
    label: "PDF Signature",
    icon: Signature,
    chipClass: "bg-emerald-50 text-emerald-700 border-emerald-200",
    lightClass: "bg-emerald-50",
  },
};

const statusMeta: Record<TaskStatus, { label: string; className: string }> = {
  pending: {
    label: "Pending",
    className: "bg-amber-50 text-amber-700 border-amber-200",
  },
  in_progress: {
    label: "In Progress",
    className: "bg-sky-50 text-sky-700 border-sky-200",
  },
  completed: {
    label: "Completed",
    className: "bg-emerald-50 text-emerald-700 border-emerald-200",
  },
};

function getTaskAction(type: TaskType) {
  switch (type) {
    case "document":
      return "Upload Document";
    case "profile":
      return "Complete Section";
    case "signature":
      return "Open Form";
    default:
      return "Open";
  }
}

function StatCard({
  title,
  value,
  subtitle,
  icon: Icon,
}: {
  title: string;
  value: string;
  subtitle: string;
  icon: React.ElementType;
}) {
  return (
    <Card className="border-slate-200 shadow-sm">
      <CardContent className="p-5">
        <div className="flex items-start justify-between gap-4">
          <div>
            <p className="text-sm font-medium text-slate-500">{title}</p>
            <p className="mt-2 text-3xl font-semibold tracking-tight text-slate-900">{value}</p>
            <p className="mt-1 text-sm text-slate-500">{subtitle}</p>
          </div>
          <div
            className="flex h-11 w-11 items-center justify-center rounded-2xl border border-slate-200 bg-white"
            style={{ boxShadow: "0 8px 24px rgba(15,23,42,0.06)" }}
          >
            <Icon className="h-5 w-5 text-slate-700" />
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

function EmptyState({
  title,
  description,
  icon: Icon,
}: {
  title: string;
  description: string;
  icon: React.ElementType;
}) {
  return (
    <Card className="border-dashed border-slate-300 bg-slate-50/70 shadow-none">
      <CardContent className="flex flex-col items-center justify-center px-6 py-14 text-center">
        <div className="mb-4 flex h-14 w-14 items-center justify-center rounded-2xl bg-white shadow-sm">
          <Icon className="h-6 w-6 text-slate-500" />
        </div>
        <h3 className="text-lg font-semibold text-slate-900">{title}</h3>
        <p className="mt-2 max-w-md text-sm text-slate-500">{description}</p>
      </CardContent>
    </Card>
  );
}

function ResourceCard({ task }: { task: Task }) {
  const meta = typeMeta[task.type];
  const Icon = meta.icon;

  return (
    <Card className="border-slate-200 shadow-sm transition-all hover:-translate-y-0.5 hover:shadow-md">
      <CardContent className="p-5">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
          <div className="min-w-0 flex-1">
            <div className="flex flex-wrap items-center gap-2">
              <Badge variant="outline" className={meta.chipClass}>
                <Icon className="mr-1 h-3.5 w-3.5" />
                {meta.label}
              </Badge>
              <Badge variant="outline" className={statusMeta.completed.className}>
                <CheckCircle2 className="mr-1 h-3.5 w-3.5" />
                Completed
              </Badge>
            </div>
            <h3 className="mt-3 text-base font-semibold text-slate-900">{task.title}</h3>
            <p className="mt-1 text-sm leading-6 text-slate-500">{task.description}</p>
            <div className="mt-4 flex flex-wrap gap-5 text-sm text-slate-500">
              <div className="flex items-center gap-2">
                <CalendarDays className="h-4 w-4" />
                {task.dueDate}
              </div>
              <div className="flex items-center gap-2">
                <FileText className="h-4 w-4" />
                {task.documentName || task.formName || task.sectionName}
              </div>
            </div>
          </div>

          <div className="flex items-center gap-2">
            <Button variant="outline" className="rounded-xl">
              <Eye className="mr-2 h-4 w-4" />
              View
            </Button>
            <Button className="rounded-xl bg-slate-900 hover:bg-slate-800">
              <Download className="mr-2 h-4 w-4" />
              Download
            </Button>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

function TaskCard({ task, onOpen }: { task: Task; onOpen: (task: Task) => void }) {
  const meta = typeMeta[task.type];
  const Icon = meta.icon;

  return (
    <Card className="border-slate-200 shadow-sm transition-all hover:-translate-y-0.5 hover:shadow-md">
      <CardContent className="p-5">
        <div className="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
          <div className="min-w-0 flex-1">
            <div className="flex flex-wrap items-center gap-2">
              <Badge variant="outline" className={meta.chipClass}>
                <Icon className="mr-1 h-3.5 w-3.5" />
                {meta.label}
              </Badge>
              <Badge variant="outline" className={statusMeta[task.status].className}>
                {statusMeta[task.status].label}
              </Badge>
            </div>

            <div className="mt-3 flex items-start gap-3">
              <div className={`mt-0.5 hidden h-11 w-11 shrink-0 items-center justify-center rounded-2xl ${meta.lightClass} sm:flex`}>
                <Icon className="h-5 w-5 text-slate-700" />
              </div>
              <div className="min-w-0 flex-1">
                <h3 className="text-base font-semibold text-slate-900">{task.title}</h3>
                <p className="mt-1 text-sm leading-6 text-slate-500">{task.description}</p>

                <div className="mt-4 flex flex-wrap gap-5 text-sm text-slate-500">
                  <div className="flex items-center gap-2">
                    <Clock3 className="h-4 w-4" />
                    Due: {task.dueDate}
                  </div>
                  <div className="flex items-center gap-2">
                    <ShieldCheck className="h-4 w-4" />
                    Requested by: {task.requestedBy}
                  </div>
                </div>
              </div>
            </div>
          </div>

          <div className="flex shrink-0 items-center gap-2">
            <Button variant="outline" className="rounded-xl" onClick={() => onOpen(task)}>
              View Details
            </Button>
            <Button className="rounded-xl bg-slate-900 hover:bg-slate-800" onClick={() => onOpen(task)}>
              {getTaskAction(task.type)}
              <ChevronRight className="ml-2 h-4 w-4" />
            </Button>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

function DetailDialog({ task, open, onOpenChange }: { task: Task | null; open: boolean; onOpenChange: (open: boolean) => void }) {
  if (!task) return null;

  const meta = typeMeta[task.type];
  const Icon = meta.icon;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-h-[92vh] overflow-hidden rounded-3xl border-slate-200 p-0 sm:max-w-5xl">
        <div className="grid max-h-[92vh] grid-cols-1 overflow-hidden lg:grid-cols-[320px_minmax(0,1fr)]">
          <div className="border-b border-slate-200 bg-slate-50 p-6 lg:border-b-0 lg:border-r">
            <div className="flex items-center gap-3">
              <div className={`flex h-12 w-12 items-center justify-center rounded-2xl ${meta.lightClass}`}>
                <Icon className="h-5 w-5 text-slate-800" />
              </div>
              <div>
                <p className="text-sm font-medium text-slate-500">Task Details</p>
                <h2 className="text-lg font-semibold text-slate-900">{meta.label}</h2>
              </div>
            </div>

            <div className="mt-6 space-y-4">
              <div>
                <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">Title</p>
                <p className="mt-1 text-sm font-medium text-slate-900">{task.title}</p>
              </div>
              <div>
                <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">Description</p>
                <p className="mt-1 text-sm leading-6 text-slate-600">{task.description}</p>
              </div>
              <div>
                <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">Requested By</p>
                <p className="mt-1 text-sm text-slate-900">{task.requestedBy}</p>
              </div>
              <div>
                <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">Due Date</p>
                <p className="mt-1 text-sm text-slate-900">{task.dueDate}</p>
              </div>
              <div>
                <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">Status</p>
                <div className="mt-2">
                  <Badge variant="outline" className={statusMeta[task.status].className}>
                    {statusMeta[task.status].label}
                  </Badge>
                </div>
              </div>
            </div>
          </div>

          <ScrollArea className="max-h-[92vh]">
            <div className="p-6 sm:p-8">
              <DialogHeader className="mb-6 space-y-2 text-left">
                <DialogTitle className="text-2xl font-semibold tracking-tight text-slate-950">{task.title}</DialogTitle>
                <DialogDescription className="text-sm leading-6 text-slate-500">
                  Complete the requested action below. This area is intentionally designed as the provider work surface.
                </DialogDescription>
              </DialogHeader>

              {task.type === "document" && (
                <div className="space-y-6">
                  <Card className="border-slate-200 shadow-sm">
                    <CardHeader>
                      <CardTitle className="text-base">Requested Document</CardTitle>
                      <CardDescription>{task.documentName || "Document upload request"}</CardDescription>
                    </CardHeader>
                    <CardContent className="space-y-5">
                      <div className="rounded-2xl border-2 border-dashed border-slate-300 bg-slate-50 p-8 text-center">
                        <div className="mx-auto flex h-14 w-14 items-center justify-center rounded-2xl bg-white shadow-sm">
                          <Upload className="h-6 w-6 text-slate-700" />
                        </div>
                        <h3 className="mt-4 text-lg font-semibold text-slate-900">Drag and drop file here</h3>
                        <p className="mt-2 text-sm text-slate-500">
                          Or click to browse from your device. Supports PDF, JPG, PNG, and DOC files.
                        </p>
                        <div className="mt-5 flex justify-center gap-3">
                          <Button className="rounded-xl bg-slate-900 hover:bg-slate-800">Choose File</Button>
                          <Button variant="outline" className="rounded-xl">View Instructions</Button>
                        </div>
                      </div>

                      <div className="rounded-2xl border border-slate-200 bg-white p-4">
                        <div className="flex items-start gap-3">
                          <AlertCircle className="mt-0.5 h-4 w-4 text-amber-600" />
                          <div>
                            <p className="text-sm font-medium text-slate-900">Before uploading</p>
                            <p className="mt-1 text-sm text-slate-500">
                              Make sure the document is current, readable, and includes all pages. Blurry uploads are the fastest way to create extra work for everybody.
                            </p>
                          </div>
                        </div>
                      </div>
                    </CardContent>
                  </Card>
                </div>
              )}

              {task.type === "profile" && (
                <div className="space-y-6">
                  <Card className="border-slate-200 shadow-sm">
                    <CardHeader>
                      <CardTitle className="text-base">Profile Section Form</CardTitle>
                      <CardDescription>{task.sectionName || "Provider profile information"}</CardDescription>
                    </CardHeader>
                    <CardContent>
                      <div className="grid grid-cols-1 gap-5 md:grid-cols-2">
                        {(task.fields || []).map((field) => (
                          <div key={field.label} className="space-y-2">
                            <label className="text-sm font-medium text-slate-700">
                              {field.label}
                              {field.required && <span className="ml-1 text-rose-500">*</span>}
                            </label>
                            <Input
                              defaultValue={field.value || ""}
                              placeholder={field.placeholder || `Enter ${field.label}`}
                              className="h-11 rounded-xl border-slate-300"
                            />
                          </div>
                        ))}
                      </div>

                      <div className="mt-5 space-y-2">
                        <label className="text-sm font-medium text-slate-700">Additional Notes</label>
                        <Textarea
                          placeholder="Add any context or explanation for the credentialing team"
                          className="min-h-[120px] rounded-2xl border-slate-300"
                        />
                      </div>
                    </CardContent>
                  </Card>
                </div>
              )}

              {task.type === "signature" && (
                <div className="space-y-6">
                  <Card className="border-slate-200 shadow-sm">
                    <CardHeader>
                      <CardTitle className="text-base">PDF Form Viewer</CardTitle>
                      <CardDescription>{task.formName || "Signature form"}</CardDescription>
                    </CardHeader>
                    <CardContent>
                      <div className="rounded-3xl border border-slate-200 bg-slate-50 p-4">
                        <div className="flex min-h-[500px] flex-col items-center justify-center rounded-2xl border-2 border-dashed border-slate-300 bg-white text-center">
                          <div className="flex h-16 w-16 items-center justify-center rounded-2xl bg-slate-100">
                            <FileBadge2 className="h-8 w-8 text-slate-700" />
                          </div>
                          <h3 className="mt-4 text-lg font-semibold text-slate-900">Nutrient PDF Viewer Placeholder</h3>
                          <p className="mt-2 max-w-lg text-sm leading-6 text-slate-500">
                            Embed the Nutrient viewer here for form review, field completion, and provider signature flow. This placeholder reserves the full work area for that experience.
                          </p>
                          <div className="mt-5 flex flex-wrap justify-center gap-3">
                            <Button className="rounded-xl bg-slate-900 hover:bg-slate-800">Open Full Viewer</Button>
                            <Button variant="outline" className="rounded-xl">Preview Form Details</Button>
                          </div>
                        </div>
                      </div>
                    </CardContent>
                  </Card>
                </div>
              )}

              <div className="mt-8 flex flex-wrap justify-end gap-3">
                <Button variant="outline" className="rounded-xl">Save Draft</Button>
                <Button className="rounded-xl bg-slate-900 hover:bg-slate-800">Submit Task</Button>
              </div>
            </div>
          </ScrollArea>
        </div>
      </DialogContent>
    </Dialog>
  );
}

export default function ProviderOnboardingPortalUI() {
  const [query, setQuery] = useState("");
  const [typeFilter, setTypeFilter] = useState<string>("all");
  const [selectedTask, setSelectedTask] = useState<Task | null>(null);
  const [open, setOpen] = useState(false);

  const pendingTasks = tasks.filter((task) => task.status !== "completed");
  const completedTasks = tasks.filter((task) => task.status === "completed");

  const completionRate = Math.round((completedTasks.length / tasks.length) * 100);

  const filteredPendingTasks = useMemo(() => {
    return pendingTasks.filter((task) => {
      const matchesQuery =
        task.title.toLowerCase().includes(query.toLowerCase()) ||
        task.description.toLowerCase().includes(query.toLowerCase());
      const matchesType = typeFilter === "all" || task.type === typeFilter;
      return matchesQuery && matchesType;
    });
  }, [query, typeFilter, pendingTasks]);

  const filteredCompletedTasks = useMemo(() => {
    return completedTasks.filter((task) => {
      const matchesQuery =
        task.title.toLowerCase().includes(query.toLowerCase()) ||
        task.description.toLowerCase().includes(query.toLowerCase());
      const matchesType = typeFilter === "all" || task.type === typeFilter;
      return matchesQuery && matchesType;
    });
  }, [query, typeFilter, completedTasks]);

  const completedDocuments = completedTasks.filter((t) => t.type === "document");
  const completedProfileInfo = completedTasks.filter((t) => t.type === "profile");
  const completedSignedForms = completedTasks.filter((t) => t.type === "signature");

  const openTask = (task: Task) => {
    setSelectedTask(task);
    setOpen(true);
  };

  return (
    <div className="min-h-screen bg-gradient-to-b from-slate-50 to-white text-slate-900">
      <div className="mx-auto max-w-7xl px-4 py-6 sm:px-6 lg:px-8 lg:py-8">
        <Card className="overflow-hidden border-slate-200 shadow-sm">
          <div
            className="relative border-b border-white/10 bg-slate-950 px-6 py-8 sm:px-8"
            style={{ backgroundImage: `linear-gradient(135deg, ${THEME} 0%, #0f172a 55%, #111827 100%)` }}
          >
            <div className="absolute inset-0 bg-[radial-gradient(circle_at_top_right,rgba(255,255,255,0.16),transparent_28%),radial-gradient(circle_at_bottom_left,rgba(255,255,255,0.10),transparent_22%)]" />
            <div className="relative flex flex-col gap-6 lg:flex-row lg:items-center lg:justify-between">
              <div className="flex items-start gap-4">
                <Avatar className="h-14 w-14 border border-white/20 bg-white/10">
                  <AvatarFallback className="bg-white/10 text-base font-semibold text-white">DR</AvatarFallback>
                </Avatar>
                <div>
                  <div className="flex flex-wrap items-center gap-2">
                    <Badge className="border-white/15 bg-white/10 text-white hover:bg-white/10">
                      <Sparkles className="mr-1.5 h-3.5 w-3.5" />
                      Provider Onboarding Portal
                    </Badge>
                  </div>
                  <h1 className="mt-3 text-3xl font-semibold tracking-tight text-white sm:text-4xl">
                    Welcome, Dr. Ravi Teja
                  </h1>
                  <p className="mt-2 max-w-2xl text-sm leading-6 text-slate-200 sm:text-base">
                    Complete your assigned onboarding tasks, securely upload documents, fill required profile information, and sign forms in one place without hunting through ten tabs like it’s 2012.
                  </p>
                </div>
              </div>

              <div className="w-full rounded-3xl border border-white/15 bg-white/10 p-5 backdrop-blur-sm lg:max-w-sm">
                <div className="flex items-center justify-between">
                  <p className="text-sm font-medium text-slate-200">Onboarding Progress</p>
                  <p className="text-sm font-semibold text-white">{completionRate}%</p>
                </div>
                <Progress value={completionRate} className="mt-3 h-2.5 bg-white/15" />
                <div className="mt-4 grid grid-cols-3 gap-3 text-center">
                  <div className="rounded-2xl bg-white/10 p-3">
                    <p className="text-xl font-semibold text-white">{pendingTasks.length}</p>
                    <p className="text-xs text-slate-200">Open Tasks</p>
                  </div>
                  <div className="rounded-2xl bg-white/10 p-3">
                    <p className="text-xl font-semibold text-white">{completedTasks.length}</p>
                    <p className="text-xs text-slate-200">Completed</p>
                  </div>
                  <div className="rounded-2xl bg-white/10 p-3">
                    <p className="text-xl font-semibold text-white">3</p>
                    <p className="text-xs text-slate-200">Task Types</p>
                  </div>
                </div>
              </div>
            </div>
          </div>

          <CardContent className="p-6 sm:p-8">
            <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
              <StatCard title="Pending Tasks" value={String(pendingTasks.length)} subtitle="Need your action" icon={Clock3} />
              <StatCard title="Completed Tasks" value={String(completedTasks.length)} subtitle="Already submitted" icon={CheckCircle2} />
              <StatCard title="Documents Vault" value={String(completedDocuments.length)} subtitle="Uploaded files" icon={FolderOpen} />
              <StatCard title="Signed Forms" value={String(completedSignedForms.length)} subtitle="Finalized PDFs" icon={FileCheck2} />
            </div>
          </CardContent>
        </Card>

        <Tabs defaultValue="tasks" className="mt-8 space-y-6">
          <div className="flex flex-col gap-4 xl:flex-row xl:items-center xl:justify-between">
            <TabsList className="h-auto flex-wrap rounded-2xl border border-slate-200 bg-white p-1.5 shadow-sm">
              <TabsTrigger value="tasks" className="rounded-xl px-4 py-2.5">Assigned Tasks</TabsTrigger>
              <TabsTrigger value="completed" className="rounded-xl px-4 py-2.5">Completed Tasks</TabsTrigger>
              <TabsTrigger value="documents" className="rounded-xl px-4 py-2.5">Documents</TabsTrigger>
              <TabsTrigger value="profile" className="rounded-xl px-4 py-2.5">Profile Information</TabsTrigger>
              <TabsTrigger value="signed" className="rounded-xl px-4 py-2.5">Signed Documents</TabsTrigger>
            </TabsList>

            <div className="flex flex-col gap-3 sm:flex-row">
              <div className="relative min-w-[280px]">
                <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-slate-400" />
                <Input
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                  placeholder="Search tasks, forms, or documents"
                  className="h-11 rounded-xl border-slate-300 bg-white pl-9"
                />
              </div>
              <div className="flex items-center gap-2">
                <div className="flex h-11 items-center rounded-xl border border-slate-300 bg-white px-3 text-slate-500">
                  <Filter className="h-4 w-4" />
                </div>
                <Select value={typeFilter} onValueChange={setTypeFilter}>
                  <SelectTrigger className="h-11 w-[210px] rounded-xl border-slate-300 bg-white">
                    <SelectValue placeholder="Filter by task type" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">All task types</SelectItem>
                    <SelectItem value="document">Document upload</SelectItem>
                    <SelectItem value="profile">Profile information</SelectItem>
                    <SelectItem value="signature">PDF signature</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>
          </div>

          <TabsContent value="tasks" className="space-y-4">
            {filteredPendingTasks.length === 0 ? (
              <EmptyState
                icon={ClipboardList}
                title="No open tasks found"
                description="Everything here is clear. Either you finished it all, or your filters are being a little too aggressive."
              />
            ) : (
              filteredPendingTasks.map((task) => <TaskCard key={task.id} task={task} onOpen={openTask} />)
            )}
          </TabsContent>

          <TabsContent value="completed" className="space-y-4">
            {filteredCompletedTasks.length === 0 ? (
              <EmptyState
                icon={CheckCircle2}
                title="No completed tasks yet"
                description="Once tasks are submitted, they will appear here with easy access for review and download."
              />
            ) : (
              filteredCompletedTasks.map((task) => <ResourceCard key={task.id} task={task} />)
            )}
          </TabsContent>

          <TabsContent value="documents" className="space-y-4">
            {completedDocuments.length === 0 ? (
              <EmptyState
                icon={FolderOpen}
                title="No uploaded documents yet"
                description="Completed document requests will be organized here so providers can easily find everything later."
              />
            ) : (
              completedDocuments.map((task) => <ResourceCard key={task.id} task={task} />)
            )}
          </TabsContent>

          <TabsContent value="profile" className="space-y-6">
            <Card className="border-slate-200 shadow-sm">
              <CardHeader>
                <CardTitle className="flex items-center gap-2 text-lg">
                  <UserCircle2 className="h-5 w-5" />
                  Provider Information Hub
                </CardTitle>
                <CardDescription>
                  All completed provider profile sections should be organized here for quick reference and future updates.
                </CardDescription>
              </CardHeader>
              <CardContent className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
                {completedProfileInfo.length === 0 ? (
                  <div className="md:col-span-2 xl:col-span-3">
                    <EmptyState
                      icon={Building2}
                      title="No completed profile sections yet"
                      description="Profile sections like DEA, license, education, and other admin-configured data will show up here once submitted."
                    />
                  </div>
                ) : (
                  completedProfileInfo.map((task) => (
                    <Card key={task.id} className="border-slate-200 shadow-sm">
                      <CardContent className="p-5">
                        <div className="flex items-start justify-between gap-3">
                          <div>
                            <Badge variant="outline" className={typeMeta.profile.chipClass}>Profile Section</Badge>
                            <h3 className="mt-3 text-base font-semibold text-slate-900">{task.sectionName}</h3>
                            <p className="mt-1 text-sm text-slate-500">Completed and available for review.</p>
                          </div>
                          <ClipboardList className="h-5 w-5 text-slate-400" />
                        </div>
                        <Separator className="my-4" />
                        <div className="space-y-3">
                          {(task.fields || []).slice(0, 3).map((field) => (
                            <div key={field.label} className="flex items-start justify-between gap-4 text-sm">
                              <span className="text-slate-500">{field.label}</span>
                              <span className="text-right font-medium text-slate-900">{field.value || "—"}</span>
                            </div>
                          ))}
                        </div>
                        <Button variant="outline" className="mt-5 w-full rounded-xl">
                          View Full Section
                        </Button>
                      </CardContent>
                    </Card>
                  ))
                )}
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="signed" className="space-y-4">
            {completedSignedForms.length === 0 ? (
              <EmptyState
                icon={Signature}
                title="No signed documents yet"
                description="Signed forms will be listed here after completion so the provider can view or download them anytime."
              />
            ) : (
              completedSignedForms.map((task) => <ResourceCard key={task.id} task={task} />)
            )}
          </TabsContent>
        </Tabs>
      </div>

      <DetailDialog task={selectedTask} open={open} onOpenChange={setOpen} />
    </div>
  );
}
