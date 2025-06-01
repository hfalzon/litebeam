package litebeam

// func TestRemoveItemFromShard(t *testing.T) {
// 	c := Config{
// 		BasePath:       "./tests",
// 		SoftCap:        2,
// 		MaxDBCount:     5,
// 		GenerationMode: "on-startup",
// 	}
// 	s, err := NewSharder(c)
// 	if err != nil {
// 		t.Error(err)
// 	}

// 	for range 20000 {
// 		_, err := s.AssignItemToShard()
// 		if err != nil {
// 			t.Error(err)
// 		}
// 		// t.Log(i)
// 	}

// 	for i := range 4231 {
// 		err := s.RemoveItemFromShard(i) //Remove 1 user
// 		if err != nil {
// 			t.Error(err)
// 		}
// 	}

// 	for range 2123 {
// 		_, err := s.AssignItemToShard()
// 		if err != nil {
// 			t.Error(err)
// 		}
// 		//t.Log(i)
// 	}

// 	time.Sleep(5 * time.Second)

// 	for range 10 {
// 		_, err := s.AssignItemToShard()
// 		if err != nil {
// 			t.Error(err)
// 		}
// 		//t.Log(i)
// 	}
// }
