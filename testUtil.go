package tsdb

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"
)

const (
	letters         = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	lengthOfLetters = len(letters)
)

var (
	DataPrefix          = DATA_PRE_FIX
	CompletePrefix      = COMPLETE_PREFIX
	LogPrefix           = LOG_FILE_PREFIX
	DataDirectory_Test  = "/tmp/path_test"
	ShardDirectory_Test = DataDirectory_Test + "/1"
)

var TestData = []TimeValuePair{
	{1440583200, 761}, {1440583261, 727}, {1440583322, 765}, {1440583378, 706}, {1440583440, 700},
	{1440583500, 679}, {1440583560, 757}, {1440583620, 708}, {1440583680, 739}, {1440583740, 707},
	{1440583800, 699}, {1440583860, 740}, {1440583920, 729}, {1440583980, 766}, {1440584040, 730},
	{1440584100, 715}, {1440584160, 705}, {1440584220, 693}, {1440584280, 765}, {1440584340, 724},
	{1440584400, 799}, {1440584460, 761}, {1440584520, 737}, {1440584580, 766}, {1440584640, 756},
	{1440584700, 719}, {1440584760, 722}, {1440584820, 801}, {1440584880, 747}, {1440584940, 731},
	{1440585000, 742}, {1440585060, 744}, {1440585120, 791}, {1440585180, 750}, {1440585240, 759},
	{1440585300, 809}, {1440585366, 751}, {1440585420, 705}, {1440585480, 770}, {1440585540, 792},
	{1440585600, 727}, {1440585660, 762}, {1440585720, 772}, {1440585780, 721}, {1440585840, 748},
	{1440585900, 753}, {1440585960, 744}, {1440586022, 716}, {1440586080, 776}, {1440586140, 659},
	{1440586200, 789}, {1440586260, 766}, {1440586320, 758}, {1440586380, 690}, {1440586440, 795},
	{1440586500, 770}, {1440586560, 758}, {1440586620, 723}, {1440586680, 767}, {1440586740, 765},
	{1440586800, 693}, {1440586860, 706}, {1440586920, 681}, {1440586980, 727}, {1440587040, 724},
	{1440587100, 780}, {1440587160, 678}, {1440587220, 696}, {1440587280, 758}, {1440587340, 740},
	{1440587400, 735}, {1440587460, 700}, {1440587520, 742}, {1440587580, 747}, {1440587640, 752},
	{1440587700, 734}, {1440587760, 743}, {1440587820, 732}, {1440587880, 746}, {1440587940, 770},
	{1440588000, 780}, {1440588060, 710}, {1440588120, 731}, {1440588180, 712}, {1440588240, 712},
	{1440588300, 741}, {1440588352, 770}, {1440588420, 770}, {1440588480, 754}, {1440588540, 718},
	{1440588600, 670}, {1440588660, 775}, {1440588720, 749}, {1440588780, 795}, {1440588840, 756},
	{1440588900, 741}, {1440588960, 787}, {1440589027, 721}, {1440589080, 745}, {1440589140, 782},
	{1440589200, 765}, {1440589260, 780}, {1440589320, 811}, {1440589380, 790}, {1440589440, 836},
	{1440589500, 743}, {1440589560, 858}, {1440589620, 739}, {1440589680, 762}, {1440589740, 770},
	{1440589800, 752}, {1440589860, 763}, {1440589920, 795}, {1440589980, 792}, {1440590040, 746},
	{1440590100, 786}, {1440590160, 785}, {1440590220, 774}, {1440590280, 786}, {1440590340, 718},
}

func FileCreate(numOfFile int) {
	err := os.MkdirAll(ShardDirectory_Test, 0777)
	if err != nil {
		fmt.Println(err)
		return
	}

	for i := 1; i <= numOfFile; i++ {
		dataFile_Test := ShardDirectory_Test + "/" + DataPrefix + "." + strconv.Itoa(i)
		completeFile_Test := ShardDirectory_Test + "/" + CompletePrefix + "." + strconv.Itoa(i)
		os.Create(dataFile_Test)
		os.Create(completeFile_Test)
	}
}

func SingleFileCreate(path string, unixTime int64) {
	dataFile := path + "/" + DataPrefix + "." + strconv.Itoa(int(unixTime))
	completeFile := path + "/" + CompletePrefix + "." + strconv.Itoa(int(unixTime))
	logFile := path + "/" + LogPrefix + "." + strconv.Itoa(int(unixTime))
	os.Create(dataFile)
	os.Create(completeFile)
	os.Create(logFile)
}

func PathCreate(shardId int32) error {
	return os.MkdirAll(fmt.Sprintf("%s/%d", DataDirectory_Test, shardId), 0755)
}

func FileDelete() {
	err := os.RemoveAll(DataDirectory_Test)
	if err != nil {
		fmt.Println(err)
		return
	}
}

func IsEqualByteSlice(s1, s2 []byte) bool {
	if len(s1) != len(s2) {
		return false
	}

	if (s1 == nil) != (s2 == nil) {
		return false
	}

	for i, v := range s1 {
		if v != s2[i] {
			return false
		}
	}
	return true
}

func IsEqualIntSlice(s1, s2 []int) bool {
	if len(s1) != len(s2) {
		return false
	}

	if (s1 == nil) != (s2 == nil) {
		return false
	}

	for i, v := range s1 {
		if v != s2[i] {
			return false
		}
	}
	return true
}

func RandStr(length int) string {
	res := make([]byte, length)
	rand.Seed(time.Now().UnixNano())

	for i := range res {
		res[i] = letters[rand.Intn(lengthOfLetters)]
	}

	return string(res)
}

func DataGenerator(numOfKeys, num int) *PutRequest {
	req := &PutRequest{}
	req.Data = make([]*DataPoint, num*numOfKeys)
	index := 0

	for i := 0; i < numOfKeys; i++ {
		testKey := RandStr(10)
		testTime := 0

		for j := 0; j < num; j++ {
			testTime += (55 + rand.Intn(10))
			newData := &DataPoint{
				Key: &Key{
					Key:     []byte(testKey),
					ShardId: int32(i + 1),
				},
				Value: &TimeValuePair{
					Timestamp: int64(testTime),
					Value:     float64(100 + rand.Intn(50)),
				},
			}
			req.Data[index] = newData
			index++
		}
	}
	return req
}
