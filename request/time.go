package request

import (
	"encoding/json"
	"time"
)

const DateLayout = "2006-01-02 15:04:05 -0700"

type Timestamp struct {
	t time.Time
}

func (st *Timestamp) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}

	t, err := time.Parse(DateLayout, s)
	if err != nil {
		return err
	}
	st.t = t
	return nil
}

func (st *Timestamp) MarshalJSON() ([]byte, error) {
	return json.Marshal(st.t.Format(DateLayout))
}

func (st *Timestamp) ToTime() time.Time {
	return st.t
}

func (st *Timestamp) String() string {
	return st.t.Format(DateLayout)
}

// UnixTimestamp handles timestamps encoded as seconds since the Unix epoch.
type UnixTimestamp struct {
	t time.Time
}

func (ut *UnixTimestamp) UnmarshalJSON(b []byte) error {
	var v float64
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	sec := int64(v)
	nsec := int64((v - float64(sec)) * float64(time.Second))
	ut.t = time.Unix(sec, nsec)
	return nil
}

func (ut *UnixTimestamp) MarshalJSON() ([]byte, error) {
	return json.Marshal(float64(ut.t.UnixNano()) / 1e9)
}

func (ut *UnixTimestamp) ToTime() time.Time { return ut.t }

func (ut *UnixTimestamp) String() string { return ut.t.Format(time.RFC3339Nano) }
