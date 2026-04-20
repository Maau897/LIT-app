create table if not exists public.usuarios_app (
    id_usuario bigint generated always as identity primary key,
    email text unique not null,
    password_hash text not null,
    aprobado boolean not null default false,
    es_admin boolean not null default false,
    rol text not null default 'captura',
    fecha_registro timestamptz not null default now()
);

create index if not exists idx_usuarios_app_email on public.usuarios_app (email);
create index if not exists idx_usuarios_app_aprobado on public.usuarios_app (aprobado);
