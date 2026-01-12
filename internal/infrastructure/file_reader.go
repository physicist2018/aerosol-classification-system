package infrastructure

import (
	"bufio"
	"errors"
	"math"
	"os"
	"strconv"
	"strings"

	"go.uber.org/zap"
	"gonum.org/v1/gonum/mat"
)

var ErrInvalidFileFormat = errors.New("invalid file format")

// MatrixData представляет данные матрицы с метками
type MatrixData struct {
	HeightLabels []float64 `gorethink:"height_labels" json:"height_labels"`
	TimeLabels   []string  `gorethink:"time_labels" json:"time_labels"`
	Rows         int       `gorethink:"rows" json:"rows"`
	Cols         int       `gorethink:"cols" json:"cols"`
	FlatData     []float64 `gorethink:"flat_data" json:"flat_data"`
}

func (m *MatrixData) Matrix() *mat.Dense {
	return mat.NewDense(m.Rows, m.Cols, m.FlatData)
}

type TXTFileReader struct {
	logger *zap.Logger
}

func NewTXTFileReader(logger *zap.Logger) *TXTFileReader {
	return &TXTFileReader{logger: logger}
}

// ReadMatrix читает матрицу из строкового содержимого файла
func (r *TXTFileReader) ReadMatrix(content string) (*MatrixData, error) {
	if content == "" {
		return nil, ErrInvalidFileFormat
	}

	var lines []string
	scanner := bufio.NewScanner(strings.NewReader(content))
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	if len(lines) < 2 {
		return nil, ErrInvalidFileFormat
	}

	// Парсим первую строку (метки времени)
	timeLabels := strings.Fields(lines[0])[1:] // Пропускаем первый элемент

	var heightLabels []float64
	var rawData [][]float64

	// Парсим остальные строки
	for i := 1; i < len(lines); i++ {
		fields := strings.Fields(lines[i])
		if len(fields) < 2 {
			continue
		}

		// Первый столбец - метка высоты
		height, err := strconv.ParseFloat(fields[0], 64)
		if err != nil {
			return nil, err
		}
		heightLabels = append(heightLabels, height)

		// Остальные столбцы - данные
		var row []float64
		for j := 1; j < len(fields); j++ {
			value, err := strconv.ParseFloat(fields[j], 64)
			if err != nil {
				return nil, err
			}
			if value < 0 {
				r.logger.Warn("Negative value found, replaced with NaN", zap.Float64("value", value))
				value = math.NaN()
			}
			row = append(row, value)
		}
		rawData = append(rawData, row)
	}

	// Создаем mat.Dense из rawData
	rows := len(rawData)
	if rows == 0 {
		return &MatrixData{
			HeightLabels: heightLabels,
			TimeLabels:   timeLabels,
			FlatData:     nil,
		}, nil
	}

	cols := len(rawData[0])
	flatData := make([]float64, 0, rows*cols)

	for _, row := range rawData {
		flatData = append(flatData, row...)
	}

	//matrix := mat.NewDense(rows, cols, flatData)

	return &MatrixData{
		HeightLabels: heightLabels,
		TimeLabels:   timeLabels,
		FlatData:     flatData,
		Rows:         rows,
		Cols:         cols,
	}, nil
}

// ReadMatrixFromFile сохраняем как отдельный метод для обратной совместимости
func (r *TXTFileReader) ReadMatrixFromFile(filename string) (*MatrixData, error) {
	content, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return r.ReadMatrix(string(content))
}
