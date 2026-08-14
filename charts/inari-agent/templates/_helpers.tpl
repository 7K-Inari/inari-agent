{{- define "inari-agent.labels" -}}
app.kubernetes.io/name: inari-agent
app.kubernetes.io/part-of: inari
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
