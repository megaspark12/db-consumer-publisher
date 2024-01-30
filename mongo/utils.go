package mongo

func WaitUntilDone[T any](resultChan chan T, errChan chan error) (result []T, err error) {
	for {
		select {
		case err = <-errChan:
			return nil, err
		case res, open := <-resultChan:
			if !open {
				return
			}
			result = append(result, res)
		}
	}
}
