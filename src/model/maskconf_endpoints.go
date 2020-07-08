package model

type MaskconfEndpoints struct {
	Id       int64  `json:"id"`
	MaskId   int64  `json:"mask_id"`
	Endpoint string `json:"endpoint"`
}

type MaskconfNids struct {
	Id     int64  `json:"id"`
	MaskId int64  `json:"mask_id"`
	Nid    string `json:"nid"`
	Path   string `json:"path"`
}
