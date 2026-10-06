# Leafrun Workspace Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Добавить отдельно проверяемый фундамент PostgreSQL: пространства, членство и метаданные проектов с запретом доступа к чужому workspace.

**Architecture:** Новая библиотека `internal/workspaces` принимает `database/sql` соединение с ограниченной ролью. Каждая операция использует транзакционный контекст пользователя и пространства; PostgreSQL RLS дополнительно ограничивает строки. Действующий renderer продолжает работать отдельно, пока следующие планы не добавят аутентификацию, Git-операции и публичные маршруты SaaS.

**Tech Stack:** Go 1.27, `database/sql`, PostgreSQL 18; драйвер [pgx v5.7.6](https://github.com/jackc/pgx/releases/tag/v5.7.6) для интеграционных проверок. В ходе реализации проверить совместимость и доступные исправления зависимостей; версия здесь задаёт исходную воспроизводимую точку.

---

Это первый самостоятельный подраздел этапа 1 [roadmap](../../architecture/roadmap-v1.md), а не план всей платформы. Новый HTTP API и миграции аккаунтов, сессий, очереди, квот и аудита получат следующие планы. Метаданные проектов в этой проверке не создают репозиторий и не дают пользователю вход: библиотека пока не подключается к публичному серверу. Требования к RLS сверены с [официальной документацией PostgreSQL](https://www.postgresql.org/docs/current/ddl-rowsecurity.html).

## Файлы и границы

| Файл | Действие | Ответственность |
|---|---|---|
| `internal/workspaces/schema/001_foundation.sql` | Создать | Первая миграция, связи и RLS |
| `internal/workspaces/store.go` | Создать | Проверка роли БД, транзакция доступа, чтение метаданных |
| `internal/workspaces/store_integration_test.go` | Создать | Негативные проверки на настоящем PostgreSQL |
| `go.mod`, `go.sum` | Обновить | Зафиксировать драйвер проверки |

Миграцию выполняет отдельная роль-владелец. `leafrun_app` не имеет superuser, BYPASSRLS, членства в других ролях, владения таблицами или CREATE в public. Контекст `userID` приходит от будущей проверенной сессии, а не из тела запроса. Никакие публичные маршруты не добавляются до появления аутентификации.

## Task 1: Подготовить изолированный тест и зависимость

- [ ] Проверить существующий baseline:

```powershell
go test ./...
```

Ожидание: PASS текущего renderer. Эти проверки выполняются при реализации, а не считаются результатом данного документа.

- [ ] Зафиксировать драйвер:

```powershell
go get github.com/jackc/pgx/v5/stdlib@v5.7.6
```

- [ ] Запустить только отдельный тестовый PostgreSQL:

```powershell
docker run --name leafrun-foundation-test -d -p 127.0.0.1:55432:5432 -e POSTGRES_DB=leafrun_foundation_test -e POSTGRES_PASSWORD=leafrun-test-local postgres:18
docker exec leafrun-foundation-test pg_isready -U postgres -d leafrun_foundation_test
$env:TEST_DATABASE_URL='postgres://postgres:leafrun-test-local@localhost:55432/leafrun_foundation_test?sslmode=disable'
$env:TEST_APP_DATABASE_URL='postgres://leafrun_app:leafrun-test-local@localhost:55432/leafrun_foundation_test?sslmode=disable'
```

Ожидание: `accepting connections`. При ещё не готовой БД повторить `pg_isready`; не выполнять миграции на production. Пароль здесь только для одноразовой локальной проверки.

## Task 2: Написать интеграционную проверку до реализации

- [ ] Создать `internal/workspaces/store_integration_test.go` с полным содержимым:

```go
//go:build integration

package workspaces

import (
    "context"
    "database/sql"
    "errors"
    "os"
    "path/filepath"
    "sync"
    "testing"
    "time"

    _ "github.com/jackc/pgx/v5/stdlib"
)

const (
    userA = "00000000-0000-0000-0000-000000000001"
    userB = "00000000-0000-0000-0000-000000000002"
    viewerA = "00000000-0000-0000-0000-000000000003"
    spaceA = "11111111-1111-1111-1111-111111111111"
    spaceB = "22222222-2222-2222-2222-222222222222"
)

func openTestDB(t *testing.T, key string) *sql.DB {
    t.Helper()
    dsn := os.Getenv(key)
    if dsn == "" { t.Fatalf("%s must name the isolated test database", key) }
    db, err := sql.Open("pgx", dsn)
    if err != nil { t.Fatal(err) }
    t.Cleanup(func() { _ = db.Close() })
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    var name string
    if err := db.QueryRowContext(ctx, "SELECT current_database()").Scan(&name); err != nil { t.Fatal(err) }
    if name != "leafrun_foundation_test" { t.Fatalf("refuse fixture reset in %q", name) }
    return db
}

func TestWorkspaceFoundation(t *testing.T) {
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    admin := openTestDB(t, "TEST_DATABASE_URL")
    _, err := admin.ExecContext(ctx, `
        DROP SCHEMA public CASCADE;
        CREATE SCHEMA public;
        REVOKE CREATE ON SCHEMA public FROM PUBLIC;
        DO $$ BEGIN
            IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='leafrun_app') THEN
                CREATE ROLE leafrun_app LOGIN PASSWORD 'leafrun-test-local'
                    NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS;
            END IF;
        END $$;
    `)
    if err != nil { t.Fatal(err) }
    migration, err := os.ReadFile(filepath.Join("schema", "001_foundation.sql"))
    if err != nil { t.Fatal(err) }
    tx, err := admin.BeginTx(ctx, nil)
    if err != nil { t.Fatal(err) }
    if _, err = tx.ExecContext(ctx, string(migration)); err != nil {
        _ = tx.Rollback(); t.Fatal(err)
    }
    if err = tx.Commit(); err != nil { t.Fatal(err) }
    _, err = admin.ExecContext(ctx, `
        INSERT INTO users (id,email) VALUES
          ('00000000-0000-0000-0000-000000000001','a@example.test'),
          ('00000000-0000-0000-0000-000000000002','b@example.test'),
          ('00000000-0000-0000-0000-000000000003','viewer@example.test');
        INSERT INTO workspaces (id,name) VALUES
          ('11111111-1111-1111-1111-111111111111','A'),
          ('22222222-2222-2222-2222-222222222222','B');
        INSERT INTO memberships (workspace_id,user_id,role) VALUES
          ('11111111-1111-1111-1111-111111111111','00000000-0000-0000-0000-000000000001','owner'),
          ('22222222-2222-2222-2222-222222222222','00000000-0000-0000-0000-000000000002','owner'),
          ('11111111-1111-1111-1111-111111111111','00000000-0000-0000-0000-000000000003','viewer');
        INSERT INTO projects (id,workspace_id,name) VALUES
          ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa','11111111-1111-1111-1111-111111111111','A document'),
          ('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb','22222222-2222-2222-2222-222222222222','B document');
    `)
    if err != nil { t.Fatal(err) }
    app := openTestDB(t, "TEST_APP_DATABASE_URL")
    store, err := New(ctx, app)
    if err != nil { t.Fatal(err) }

    t.Run("unsafe database role is refused", func(t *testing.T) {
        if _, err := New(ctx, admin); !errors.Is(err, ErrUnsafeRole) { t.Fatalf("got %v", err) }
    })
    t.Run("a second owner is refused", func(t *testing.T) {
        if _, err := admin.ExecContext(ctx, "UPDATE memberships SET role='owner' WHERE workspace_id=$1 AND user_id=$2", spaceA, viewerA); err == nil { t.Fatal("second owner accepted") }
    })
    t.Run("member sees only own project", func(t *testing.T) {
        items, err := store.ListProjects(ctx, userA, spaceA)
        if err != nil || len(items) != 1 || items[0].Name != "A document" { t.Fatalf("%v, %v", items, err) }
        if _, err := store.ListProjects(ctx, userA, spaceB); !errors.Is(err, ErrForbidden) { t.Fatalf("got %v", err) }
    })
    t.Run("RLS blocks accidental missing workspace filter", func(t *testing.T) {
        tx, err := store.BeginTenant(ctx, userA, spaceA)
        if err != nil { t.Fatal(err) }
        defer tx.Rollback()
        var count int
        if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM projects").Scan(&count); err != nil || count != 1 { t.Fatalf("%d, %v", count, err) }
        if _, err := tx.ExecContext(ctx, "INSERT INTO projects(id,workspace_id,name) VALUES('cccccccc-cccc-cccc-cccc-cccccccccccc',$1,'foreign')", spaceB); err == nil { t.Fatal("foreign write accepted") }
    })
    t.Run("viewer cannot write", func(t *testing.T) {
        tx, err := store.BeginTenant(ctx, viewerA, spaceA)
        if err != nil { t.Fatal(err) }
        defer tx.Rollback()
        if _, err := tx.ExecContext(ctx, "INSERT INTO projects(id,workspace_id,name) VALUES('dddddddd-dddd-dddd-dddd-dddddddddddd',$1,'viewer write')", spaceA); err == nil { t.Fatal("viewer write accepted") }
    })
    t.Run("context is cleared on reused connection", func(t *testing.T) {
        app.SetMaxOpenConns(1)
        app.SetMaxIdleConns(1)
        if _, err := store.ListProjects(ctx, userA, spaceA); err != nil { t.Fatal(err) }
        var count int
        if err := app.QueryRowContext(ctx, "SELECT count(*) FROM projects").Scan(&count); err != nil || count != 0 { t.Fatalf("context leaked: %d, %v", count, err) }
    })
    t.Run("concurrent tenants do not mix", func(t *testing.T) {
        app.SetMaxOpenConns(4)
        failures := make(chan error, 20)
        var wg sync.WaitGroup
        for i := 0; i < 20; i++ {
            wg.Add(1)
            go func(i int) {
                defer wg.Done()
                user, space, name := userA, spaceA, "A document"
                if i%2 == 1 { user, space, name = userB, spaceB, "B document" }
                items, err := store.ListProjects(ctx, user, space)
                if err != nil { failures <- err; return }
                if len(items) != 1 || items[0].Name != name { failures <- errors.New("tenant data mixed") }
            }(i)
        }
        wg.Wait()
        close(failures)
        for err := range failures { t.Error(err) }
    })
    t.Run("database role membership is refused", func(t *testing.T) {
        if _, err := admin.ExecContext(ctx, "GRANT postgres TO leafrun_app"); err != nil { t.Fatal(err) }
        defer func() {
            if _, err := admin.ExecContext(ctx, "REVOKE postgres FROM leafrun_app"); err != nil { t.Error(err) }
        }()
        if _, err := New(ctx, app); !errors.Is(err, ErrUnsafeRole) { t.Fatalf("got %v", err) }
    })
}
```

- [ ] Запустить:

```powershell
go test -tags=integration ./internal/workspaces -run TestWorkspaceFoundation -count=1 -v
```

Ожидание до реализации: FAIL из-за отсутствия `New`, `ErrForbidden` и `ErrUnsafeRole`. Не заменять это mock-БД: проверка должна поймать ошибки настоящих RLS-политик и повторного использования соединения.

## Task 3: Создать миграцию

- [ ] Создать `internal/workspaces/schema/001_foundation.sql`:

```sql
CREATE TABLE users (
    id uuid PRIMARY KEY,
    email text NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE workspaces (
    id uuid PRIMARY KEY,
    name text NOT NULL CHECK (length(name) BETWEEN 1 AND 200),
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE memberships (
    workspace_id uuid NOT NULL REFERENCES workspaces(id),
    user_id uuid NOT NULL REFERENCES users(id),
    role text NOT NULL CHECK (role IN ('owner','admin','editor','viewer')),
    PRIMARY KEY (workspace_id,user_id)
);
CREATE UNIQUE INDEX single_workspace_owner ON memberships(workspace_id) WHERE role='owner';
CREATE TABLE projects (
    id uuid PRIMARY KEY,
    workspace_id uuid NOT NULL REFERENCES workspaces(id),
    name text NOT NULL CHECK (length(name) BETWEEN 1 AND 200),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (workspace_id,id)
);

ALTER TABLE users ENABLE ROW LEVEL SECURITY;
ALTER TABLE users FORCE ROW LEVEL SECURITY;
ALTER TABLE workspaces ENABLE ROW LEVEL SECURITY;
ALTER TABLE workspaces FORCE ROW LEVEL SECURITY;
ALTER TABLE memberships ENABLE ROW LEVEL SECURITY;
ALTER TABLE memberships FORCE ROW LEVEL SECURITY;
ALTER TABLE projects ENABLE ROW LEVEL SECURITY;
ALTER TABLE projects FORCE ROW LEVEL SECURITY;

CREATE POLICY user_self ON users FOR SELECT TO leafrun_app USING (
    id = nullif(current_setting('leafrun.user_id',true),'')::uuid
);
CREATE POLICY membership_self ON memberships FOR SELECT TO leafrun_app USING (
    user_id = nullif(current_setting('leafrun.user_id',true),'')::uuid
    AND workspace_id = nullif(current_setting('leafrun.workspace_id',true),'')::uuid
);
CREATE POLICY workspace_member ON workspaces FOR SELECT TO leafrun_app USING (
    id = nullif(current_setting('leafrun.workspace_id',true),'')::uuid
    AND EXISTS (SELECT 1 FROM memberships m WHERE m.workspace_id = workspaces.id)
);
CREATE POLICY project_read ON projects FOR SELECT TO leafrun_app USING (
    workspace_id = nullif(current_setting('leafrun.workspace_id',true),'')::uuid
    AND EXISTS (SELECT 1 FROM memberships m WHERE m.workspace_id = projects.workspace_id)
);
CREATE POLICY project_insert ON projects FOR INSERT TO leafrun_app WITH CHECK (
    workspace_id = nullif(current_setting('leafrun.workspace_id',true),'')::uuid
    AND EXISTS (SELECT 1 FROM memberships m WHERE m.workspace_id = projects.workspace_id AND m.role IN ('owner','admin','editor'))
);

GRANT USAGE ON SCHEMA public TO leafrun_app;
GRANT SELECT ON users,workspaces,memberships,projects TO leafrun_app;
GRANT INSERT ON projects TO leafrun_app;
```

Миграция применяется транзакционно. В рабочем развёртывании роль и её случайный пароль создаёт provisioning отдельно; тестовый пароль в миграцию не попадает. Для временного теста роль уже создала fixture из Task 2. Индекс запрещает второго Owner; создание пространства и передача владения следующим сервисным модулем должны гарантировать одного Owner на завершении транзакции. Публичное создание проекта появится только через Git/квоты и доверенный сервисный метод; наличие SQL-разрешения не создаёт HTTP-маршрут.

## Task 4: Реализовать транзакционный доступ

- [ ] Создать `internal/workspaces/store.go`:

```go
package workspaces

import (
    "context"
    "database/sql"
    "errors"
)

var (
    ErrForbidden = errors.New("workspace access denied")
    ErrUnsafeRole = errors.New("application database role is privileged")
)

type Store struct { db *sql.DB }
type Project struct { ID, WorkspaceID, Name string }

func New(ctx context.Context, db *sql.DB) (*Store, error) {
    var unsafe bool
    err := db.QueryRowContext(ctx, `
        SELECT r.rolsuper OR r.rolbypassrls
          OR EXISTS (SELECT 1 FROM pg_auth_members m WHERE m.member=r.oid)
          OR has_schema_privilege(current_user,'public','CREATE')
          OR EXISTS (
            SELECT 1 FROM pg_class c
            WHERE c.relnamespace='public'::regnamespace
              AND c.relname IN ('users','workspaces','memberships','projects')
              AND c.relowner=r.oid
          )
        FROM pg_roles r WHERE r.rolname=current_user
    `).Scan(&unsafe)
    if err != nil { return nil, err }
    if unsafe { return nil, ErrUnsafeRole }
    return &Store{db: db}, nil
}

func (s *Store) BeginTenant(ctx context.Context, userID, workspaceID string) (*sql.Tx, error) {
    tx, err := s.db.BeginTx(ctx, nil)
    if err != nil { return nil, err }
    fail := func(err error) (*sql.Tx, error) { _ = tx.Rollback(); return nil, err }
    _, err = tx.ExecContext(ctx, `
        SELECT set_config('leafrun.user_id',$1,true),
               set_config('leafrun.workspace_id',$2,true)
    `, userID, workspaceID)
    if err != nil { return fail(err) }
    var allowed bool
    err = tx.QueryRowContext(ctx, `
        SELECT EXISTS (SELECT 1 FROM memberships
        WHERE workspace_id=$1::uuid AND user_id=$2::uuid)
    `, workspaceID, userID).Scan(&allowed)
    if err != nil { return fail(err) }
    if !allowed { return fail(ErrForbidden) }
    return tx, nil
}

func (s *Store) ListProjects(ctx context.Context, userID, workspaceID string) ([]Project, error) {
    tx, err := s.BeginTenant(ctx, userID, workspaceID)
    if err != nil { return nil, err }
    defer tx.Rollback()
    rows, err := tx.QueryContext(ctx, `
        SELECT id::text,workspace_id::text,name FROM projects
        WHERE workspace_id=$1::uuid ORDER BY created_at,id
    `, workspaceID)
    if err != nil { return nil, err }
    projects := make([]Project, 0)
    for rows.Next() {
        var p Project
        if err := rows.Scan(&p.ID,&p.WorkspaceID,&p.Name); err != nil { _ = rows.Close(); return nil, err }
        projects = append(projects, p)
    }
    rowErr := rows.Err()
    closeErr := rows.Close()
    if rowErr != nil { return nil, rowErr }
    if closeErr != nil { return nil, closeErr }
    if err := tx.Commit(); err != nil { return nil, err }
    return projects, nil
}
```

`set_config(..., true)` ограничен транзакцией. Не заменять его на настройку соединения вне транзакции: пул тогда может передать контекст следующему пользователю. Значения параметризованы, строки SQL не собираются из входа. RLS здесь дополнение к проверке личности и роли приложения, а не способ доверять произвольному `userID` клиента.

- [ ] Отформатировать и выполнить негативные проверки:

```powershell
gofmt -w internal/workspaces/store.go internal/workspaces/store_integration_test.go
go test -tags=integration ./internal/workspaces -run TestWorkspaceFoundation -count=1 -v
```

Ожидание: все восемь дочерних проверок PASS; второй Owner, чужая запись и запись Viewer отвергнуты; запрос без контекста видит 0 проектов; 20 параллельных операций не смешивают пространства; опасное членство роли БД запрещено.

## Task 5: Проверить совместимость и зафиксировать результат

- [ ] Выполнить проверку кода и существующего движка:

```powershell
go vet ./...
go test ./...
go test -race -tags=integration ./internal/workspaces -run TestWorkspaceFoundation -count=1
```

Если локальный Go на Windows не поддерживает race из-за отсутствия C toolchain, выполнять последнюю команду в Linux CI с доступом к тестовой PostgreSQL. Не утверждать, что race прошёл, по результату обычного теста. Проверка foundation требует отдельной тестовой БД; `.env` рабочего renderer не используется.

- [ ] После PASS записать проверенные версии Go/PostgreSQL/pgx и команды в отдельный отчёт реализации. В текущем плане проверки только предписаны.

- [ ] Зафиксировать ограниченный набор файлов:

```powershell
git add go.mod go.sum internal/workspaces
git commit -m "feat: add workspace schema and tenant transactions"
```

- [ ] Убрать только созданный тестовый контейнер и временные переменные:

```powershell
docker rm -f leafrun-foundation-test
Remove-Item Env:TEST_DATABASE_URL -ErrorAction SilentlyContinue
Remove-Item Env:TEST_APP_DATABASE_URL -ErrorAction SilentlyContinue
```

## Следующая граница

После этой библиотеки проектируем и реализуем регистрацию, подтверждение email, серверную сессию и создание личного пространства через ограниченный сервисный путь с аудитом. Затем managed-проект и редактор, потом очередь и отдельная среда сборки. Эти функции требуют самостоятельных планов и проверок; foundation не считается готовым пользовательским MVP.
