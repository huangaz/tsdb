package testUtil

import (
	"fmt"
	"github.com/huangaz/tsdb/lib/dataTypes"
	"os"
	"strconv"
)

var (
	DataPrefix          = dataTypes.DATA_PRE_FIX
	CompletePrefix      = dataTypes.COMPLETE_PREFIX
	LogPrefix           = dataTypes.LOG_FILE_PREFIX
	DataDirectory_Test  = "/tmp/path_test"
	ShardDirectory_Test = DataDirectory_Test + "/1"
)

var TestData = []dataTypes.DataPoint{
	{761, 1440583200}, {727, 1440583261}, {765, 1440583322}, {706, 1440583378}, {700, 1440583440},
	{679, 1440583500}, {757, 1440583560}, {708, 1440583620}, {739, 1440583680}, {707, 1440583740},
	{699, 1440583800}, {740, 1440583860}, {729, 1440583920}, {766, 1440583980}, {730, 1440584040},
	{715, 1440584100}, {705, 1440584160}, {693, 1440584220}, {765, 1440584280}, {724, 1440584340},
	{799, 1440584400}, {761, 1440584460}, {737, 1440584520}, {766, 1440584580}, {756, 1440584640},
	{719, 1440584700}, {722, 1440584760}, {801, 1440584820}, {747, 1440584880}, {731, 1440584940},
	{742, 1440585000}, {744, 1440585060}, {791, 1440585120}, {750, 1440585180}, {759, 1440585240},
	{809, 1440585300}, {751, 1440585366}, {705, 1440585420}, {770, 1440585480}, {792, 1440585540},
	{727, 1440585600}, {762, 1440585660}, {772, 1440585720}, {721, 1440585780}, {748, 1440585840},
	{753, 1440585900}, {744, 1440585960}, {716, 1440586022}, {776, 1440586080}, {659, 1440586140},
	{789, 1440586200}, {766, 1440586260}, {758, 1440586320}, {690, 1440586380}, {795, 1440586440},
	{770, 1440586500}, {758, 1440586560}, {723, 1440586620}, {767, 1440586680}, {765, 1440586740},
	{693, 1440586800}, {706, 1440586860}, {681, 1440586920}, {727, 1440586980}, {724, 1440587040},
	{780, 1440587100}, {678, 1440587160}, {696, 1440587220}, {758, 1440587280}, {740, 1440587340},
	{735, 1440587400}, {700, 1440587460}, {742, 1440587520}, {747, 1440587580}, {752, 1440587640},
	{734, 1440587700}, {743, 1440587760}, {732, 1440587820}, {746, 1440587880}, {770, 1440587940},
	{780, 1440588000}, {710, 1440588060}, {731, 1440588120}, {712, 1440588180}, {712, 1440588240},
	{741, 1440588300}, {770, 1440588352}, {770, 1440588420}, {754, 1440588480}, {718, 1440588540},
	{670, 1440588600}, {775, 1440588660}, {749, 1440588720}, {795, 1440588780}, {756, 1440588840},
	{741, 1440588900}, {787, 1440588960}, {721, 1440589027}, {745, 1440589080}, {782, 1440589140},
	{765, 1440589200}, {780, 1440589260}, {811, 1440589320}, {790, 1440589380}, {836, 1440589440},
	{743, 1440589500}, {858, 1440589560}, {739, 1440589620}, {762, 1440589680}, {770, 1440589740},
	{752, 1440589800}, {763, 1440589860}, {795, 1440589920}, {792, 1440589980}, {746, 1440590040},
	{786, 1440590100}, {785, 1440590160}, {774, 1440590220}, {786, 1440590280}, {718, 1440590340},
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

func PathCreate(shardId int64) string {
	ShardDirectory := DataDirectory_Test + "/" + strconv.Itoa(int(shardId))
	err := os.MkdirAll(ShardDirectory, 0777)
	if err != nil {
		fmt.Println(err)
		return ""
	}
	return ShardDirectory
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
