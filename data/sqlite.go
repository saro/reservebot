package data

import (
	"database/sql"
	"time"

	"github.com/ameliagapin/reservebot/err"
	"github.com/ameliagapin/reservebot/models"
	_ "github.com/mattn/go-sqlite3"
)

// Sqlite implements Manager backed by an sqlite database. It keeps an in memory
// representation and persists all changes to the database.
type Sqlite struct {
	mem *Memory
	db  *sql.DB
}

// NewSqlite opens (or creates) an sqlite database at the given path and loads
// any existing data into memory.
func NewSqlite(path string) (*Sqlite, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}
	if err := createSqliteTables(db); err != nil {
		return nil, err
	}
	s := &Sqlite{mem: NewMemory(), db: db}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func createSqliteTables(db *sql.DB) error {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS resources (
        name TEXT NOT NULL,
        env TEXT NOT NULL,
        last_activity INTEGER,
        PRIMARY KEY(env, name)
    )`)
	if err != nil {
		return err
	}
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS reservations (
        user_id TEXT,
        user_name TEXT,
        name TEXT,
        env TEXT,
        time INTEGER
    )`)
	return err
}

func (s *Sqlite) load() error {
	rows, err := s.db.Query(`SELECT name, env, last_activity FROM resources`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var name, env string
		var last int64
		if err := rows.Scan(&name, &env, &last); err != nil {
			return err
		}
		r := &models.Resource{Name: name, Env: env, LastActivity: time.Unix(last, 0)}
		s.mem.Resources[r.Key()] = r
	}
	rows, err = s.db.Query(`SELECT user_id, user_name, name, env, time FROM reservations ORDER BY time`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var uid, uname, name, env string
		var ts int64
		if err := rows.Scan(&uid, &uname, &name, &env, &ts); err != nil {
			return err
		}
		u := &models.User{ID: uid, Name: uname}
		r := s.mem.GetResource(name, env, true)
		res := &models.Reservation{User: u, Resource: r, Time: time.Unix(ts, 0)}
		s.mem.Reservations = append(s.mem.Reservations, res)
	}
	return nil
}

func (s *Sqlite) Create(name, env string) error {
	if err := s.mem.Create(name, env); err != nil {
		return err
	}
	r := s.mem.GetResource(name, env, false)
	_, err := s.db.Exec(`INSERT OR REPLACE INTO resources(name, env, last_activity) VALUES(?,?,?)`, name, env, r.LastActivity.Unix())
	return err
}

func (s *Sqlite) Reserve(u *models.User, name, env string) error {
	if err := s.mem.Reserve(u, name, env); err != nil {
		return err
	}
	_, err := s.db.Exec(`INSERT INTO reservations(user_id, user_name, name, env, time) VALUES(?,?,?,?,?)`, u.ID, u.Name, name, env, time.Now().Unix())
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`UPDATE resources SET last_activity=? WHERE name=? AND env=?`, time.Now().Unix(), name, env)
	return err
}

func (s *Sqlite) GetReservation(u *models.User, name, env string) *models.Reservation {
	return s.mem.GetReservation(u, name, env)
}

func (s *Sqlite) Remove(u *models.User, name, env string) error {
	if err := s.mem.Remove(u, name, env); err != nil {
		return err
	}
	_, err := s.db.Exec(`DELETE FROM reservations WHERE user_id=? AND name=? AND env=?`, u.ID, name, env)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`UPDATE resources SET last_activity=? WHERE name=? AND env=?`, time.Now().Unix(), name, env)
	return err
}

func (s *Sqlite) GetPosition(u *models.User, name, env string) (int, error) {
	return s.mem.GetPosition(u, name, env)
}

func (s *Sqlite) GetResource(name, env string, create bool) *models.Resource {
	return s.mem.GetResource(name, env, create)
}

func (s *Sqlite) RemoveResource(name, env string) error {
	if err := s.mem.RemoveResource(name, env); err != nil {
		return err
	}
	_, err := s.db.Exec(`DELETE FROM reservations WHERE name=? AND env=?`, name, env)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`DELETE FROM resources WHERE name=? AND env=?`, name, env)
	return err
}

func (s *Sqlite) RemoveEnv(name, env string) error {
	if err := s.mem.RemoveEnv(name, env); err != nil {
		return err
	}
	_, err := s.db.Exec(`DELETE FROM reservations WHERE env=?`, env)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`DELETE FROM resources WHERE env=?`, env)
	return err
}

func (s *Sqlite) GetResources() []*models.Resource { return s.mem.GetResources() }
func (s *Sqlite) GetQueues() []*models.Queue       { return s.mem.GetQueues() }
func (s *Sqlite) GetQueueForResource(name, env string) (*models.Queue, error) {
	return s.mem.GetQueueForResource(name, env)
}
func (s *Sqlite) GetReservationForResource(name, env string) (*models.Reservation, error) {
	return s.mem.GetReservationForResource(name, env)
}
func (s *Sqlite) GetQueuesForEnv(env string) map[string]*models.Queue {
	return s.mem.GetQueuesForEnv(env)
}
func (s *Sqlite) GetResourcesForEnv(env string) []*models.Resource {
	return s.mem.GetResourcesForEnv(env)
}
func (s *Sqlite) GetAllUsersInQueues() []*models.User { return s.mem.GetAllUsersInQueues() }

func (s *Sqlite) ClearQueueForResource(name, env string) error {
	if err := s.mem.ClearQueueForResource(name, env); err != nil {
		return err
	}
	_, err := s.db.Exec(`DELETE FROM reservations WHERE name=? AND env=?`, name, env)
	return err
}

func (s *Sqlite) PruneInactiveResources(hours int) error {
	resources := s.mem.GetResources()
	oldest := time.Now().Add(-time.Duration(hours) * time.Hour)
	for _, r := range resources {
		q, err := s.mem.GetQueueForResource(r.Name, r.Env)
		if err != nil {
			continue
		}
		if q.HasReservations() {
			continue
		}
		if r.LastActivity.Before(oldest) {
			s.RemoveResource(r.Name, r.Env)
		}
	}
	return nil
}
