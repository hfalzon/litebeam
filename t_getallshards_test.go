package litebeam

// func TestGetAllShards(t *testing.T) {
// 	const DBCount = 10
// 	c := Config{
// 		BasePath:       "./tests",
// 		SoftCap:        100,
// 		GenerationMode: OnStartup,
// 		MaxDBCount:     DBCount,
// 	}
// 	s, err := NewSharder(c)
// 	if err != nil {
// 		t.Error(err)
// 	}

// 	as, err := s.GetAllShards()
// 	if err != nil {
// 		t.Fatalf("failed to get shards: %s", err)
// 	}

// 	if len(as) != DBCount {
// 		t.Fatalf("incorrect shard count expected %d, got %d", DBCount, len(as))
// 	}
// }
