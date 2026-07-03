{{- define "game-platform.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "game-platform.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "game-platform.labels" -}}
app.kubernetes.io/name: {{ include "game-platform.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}
{{- define "game-platform.service" -}}
{{- $name := .name }}
{{- $values := .values }}
{{- $root := .root }}
{{- $isStatefulSet := eq $name "game-room" }}
---
apiVersion: v1
kind: Service
metadata:
  name: {{ $name }}
  namespace: game-platform
  labels:
    app: {{ $name }}
spec:
  {{- if $isStatefulSet }}
  clusterIP: None
  {{- end }}
  selector:
    app: {{ $name }}
  ports:
    - port: {{ $values.port }}
      targetPort: {{ $values.port }}
      protocol: TCP
      name: http
    {{- if eq $name "nginx" }}
    - port: 443
      targetPort: 443
      protocol: TCP
      name: https
    {{- end }}
---
{{- if $isStatefulSet -}}
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: {{ $name }}
  namespace: game-platform
  labels:
    app: {{ $name }}
spec:
  serviceName: {{ $name }}
  podManagementPolicy: Parallel
{{- else -}}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ $name }}
  namespace: game-platform
  labels:
    app: {{ $name }}
spec:
{{- end }}
  replicas: {{ $values.replicas }}
  selector:
    matchLabels:
      app: {{ $name }}
  template:
    metadata:
      labels:
        app: {{ $name }}
    spec:
      {{- if eq $name "matchmaking" }}
      serviceAccountName: matchmaking
      {{- end }}
      containers:
        - name: {{ $name }}
          image: {{ $values.image }}
          imagePullPolicy: {{ $root.Values.global.imagePullPolicy }}
          ports:
            - containerPort: {{ $values.port }}
          {{- if eq $name "nginx" }}
            - containerPort: 443
              name: https
          {{- end }}
          env:
            {{- range $key, $value := $values.env }}
            - name: {{ $key }}
              value: "{{ $value }}"
            {{- end }}
            - name: OTEL_SERVICE_NAME
              value: "{{ $name }}-service"
            - name: OTEL_EXPORTER_OTLP_ENDPOINT
              value: "http://otel-collector:4318"
          {{- if and (eq $name "nginx") }}
          volumeMounts:
            - name: nginx-config
              mountPath: /etc/nginx/nginx.conf
              subPath: nginx.conf
            - name: nginx-conf-d
              mountPath: /etc/nginx/conf.d
            - name: nginx-certs
              mountPath: /etc/nginx/certs
          {{- end }}
          {{- if $values.resources }}
          resources:
            requests:
              cpu: {{ $values.resources.requests.cpu }}
              memory: {{ $values.resources.requests.memory }}
            limits:
              cpu: {{ $values.resources.limits.cpu }}
              memory: {{ $values.resources.limits.memory }}
          {{- end }}
          livenessProbe:
            httpGet:
              path: /health
              port: {{ $values.port }}
            initialDelaySeconds: 10
            periodSeconds: 15
          readinessProbe:
            httpGet:
              path: /ready
              port: {{ $values.port }}
            initialDelaySeconds: 5
            periodSeconds: 10
        {{- if and (eq $name "nginx") $root.Values.etcdWatcher.enabled }}
        - name: etcd-watcher
          image: {{ $root.Values.etcdWatcher.image }}
          imagePullPolicy: {{ $root.Values.global.imagePullPolicy }}
          ports:
            - containerPort: {{ $root.Values.etcdWatcher.port }}
          env:
            - name: ETCD_HOST
              value: "etcd"
            - name: ETCD_PORT
              value: "2379"
            - name: NGINX_CONF_DIR
              value: "/etc/nginx/conf.d"
          volumeMounts:
            - name: nginx-conf-d
              mountPath: /etc/nginx/conf.d
        {{- end }}
      {{- if eq $name "nginx" }}
      initContainers:
        - name: nginx-init
          image: alpine:3.19
          command:
            - /bin/sh
            - -c
            - |
              mkdir -p /etc/nginx/certs
              openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
                -keyout /etc/nginx/certs/server.key \
                -out /etc/nginx/certs/server.crt \
                -subj "/CN=localhost/O=Multiplayer Demo/C=US" 2>/dev/null
          volumeMounts:
            - name: nginx-certs
              mountPath: /etc/nginx/certs
      volumes:
        - name: nginx-config
          configMap:
            name: nginx-config
        - name: nginx-conf-d
          emptyDir: {}
        - name: nginx-certs
          emptyDir: {}
      {{- end }}
---
{{- if and (hasKey $values "hpa") $values.hpa.enabled }}
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: {{ $name }}-hpa
  namespace: game-platform
  labels:
    app: {{ $name }}
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: {{ if eq $name "game-room" }}StatefulSet{{ else }}Deployment{{ end }}
    name: {{ $name }}
  minReplicas: {{ $values.hpa.minReplicas }}
  maxReplicas: {{ $values.hpa.maxReplicas }}
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: {{ $values.hpa.targetCPUUtilizationPercentage }}
    - type: Resource
      resource:
        name: memory
        target:
          type: Utilization
          averageUtilization: {{ $values.hpa.targetMemoryUtilizationPercentage }}
{{- end }}
{{- end }}
