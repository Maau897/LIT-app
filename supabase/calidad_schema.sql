create table if not exists public.calidad_no_conformidades (
    id_no_conformidad bigint generated always as identity primary key,
    codigo text unique not null,
    titulo text not null,
    descripcion text not null,
    origen text not null,
    area text not null,
    severidad text not null,
    estado text not null default 'Abierta',
    detectado_por text not null,
    responsable text not null,
    fecha_deteccion date not null,
    fecha_compromiso date,
    causa_raiz text,
    verificacion_cierre text,
    fecha_cierre date,
    aprobado_por text,
    fecha_aprobacion date,
    comentario_final text,
    created_at timestamptz not null default now()
);

create table if not exists public.calidad_acciones (
    id_accion bigint generated always as identity primary key,
    id_no_conformidad bigint not null references public.calidad_no_conformidades(id_no_conformidad) on delete cascade,
    titulo text not null,
    descripcion text not null,
    tipo_accion text not null,
    responsable text not null,
    estado text not null default 'Abierta',
    fecha_inicio date not null,
    fecha_compromiso date,
    fecha_cierre date,
    verificacion_eficacia text,
    aprobado_por text,
    fecha_aprobacion date,
    comentario_final text,
    created_at timestamptz not null default now()
);

create table if not exists public.calidad_auditorias (
    id_auditoria bigint generated always as identity primary key,
    codigo text unique not null,
    titulo text not null,
    area text not null,
    auditor_lider text not null,
    fecha_programada date not null,
    alcance text,
    criterios text,
    estado text not null default 'Programada',
    resultado text,
    created_at timestamptz not null default now()
);

create table if not exists public.calidad_auditoria_hallazgos (
    id_hallazgo bigint generated always as identity primary key,
    id_auditoria bigint not null references public.calidad_auditorias(id_auditoria) on delete cascade,
    referencia text not null,
    descripcion text not null,
    severidad text not null,
    estado text not null default 'Abierto',
    responsable text,
    fecha_compromiso date,
    created_at timestamptz not null default now()
);

create table if not exists public.calidad_documentos (
    id_documento bigint generated always as identity primary key,
    codigo text unique not null,
    nombre text not null,
    proceso_area text not null,
    tipo_documento text not null,
    estado text not null default 'Borrador',
    version_actual text,
    vigente_desde date,
    vigente_hasta date,
    aprobado_por text,
    fecha_aprobacion date,
    observaciones text,
    created_at timestamptz not null default now()
);

create table if not exists public.calidad_documento_versiones (
    id_version bigint generated always as identity primary key,
    id_documento bigint not null references public.calidad_documentos(id_documento) on delete cascade,
    version text not null,
    nombre_archivo text,
    ruta_archivo text,
    cambios_resumen text,
    elaborado_por text not null,
    aprobado_por text,
    fecha_aprobacion date,
    es_vigente boolean not null default false,
    created_at timestamptz not null default now()
);

create table if not exists public.calidad_evidencias (
    id_evidencia bigint generated always as identity primary key,
    tipo_entidad text not null,
    id_entidad bigint not null,
    nombre_archivo text not null,
    ruta_archivo text not null,
    descripcion text,
    subido_por text not null,
    created_at timestamptz not null default now()
);

create table if not exists public.calidad_bitacora (
    id_evento bigint generated always as identity primary key,
    entidad_tipo text not null,
    entidad_id bigint,
    accion text not null,
    detalle text not null,
    usuario_email text not null,
    created_at timestamptz not null default now()
);

create index if not exists idx_calidad_nc_estado on public.calidad_no_conformidades (estado);
create index if not exists idx_calidad_acciones_estado on public.calidad_acciones (estado);
create index if not exists idx_calidad_auditorias_estado on public.calidad_auditorias (estado);
create index if not exists idx_calidad_documentos_estado on public.calidad_documentos (estado);
create index if not exists idx_calidad_bitacora_created_at on public.calidad_bitacora (created_at desc);
