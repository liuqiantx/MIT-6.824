package mr

import "strconv"

func isElementInSLice(ele int, s []int) {
	for _, v := range s {
		if v == ele {
			return true
		}
	}
	return false
}

func makeMapOutFileName(fileIndex int, partIndex int) string {
	return "mr-" + strconv.Itoa(fileIndex) + "-" + strconv.Itoa(partIndex)
}

func makeReduceOutFileName(partIndex int) string {
	return "mr-out-" + strconv.Itoa(partIndex)
}
