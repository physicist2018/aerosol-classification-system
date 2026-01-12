package aerosol

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"
	"gonum.org/v1/gonum/mat"
	"gonum.org/v1/gonum/optimize"
)

// Параметры для каждого типа аэрозоля
type AerosolParams struct {
	DeltaMin, DeltaMax float64
	GFMin, GFMax       float64
}

// Результат классификации для одной точки
type ClassificationResult struct {
	ND, NU, NS             float64 // Доли аэрозолей
	Sd, Su, Ss             float64 // Факторы пересчета
	DeltaD, DeltaU, DeltaS float64 // Деполяризация
	GFd, GFu, GFs          float64 // Флуоресценция
	Residual               float64
}

// Конфигурация классификатора
type ClassifierConfig struct {
	UrbanAerosol  AerosolParams
	DustAerosol   AerosolParams
	SmogAerosol   AerosolParams
	NIters        int     // Количество итераций
	BestSolutions int     // p лучших решений для усреднения
	Epsilon       float64 // Точность
	Logger        *zap.Logger
}

// Классификатор аэрозолей
type AerosolClassifier struct {
	config ClassifierConfig
}

type ClassificationResults struct {
	Residuals, Nd, Nu, Ns  *mat.Dense
	Sd, Su, Ss             float64
	DeltaD, DeltaU, DeltaS *mat.Dense
	GFd, GFu, GFs          *mat.Dense
}

type aerosolSolveSystemParams struct {
	deltaPrimeVal, gfVal                  float64
	deltaD, deltaU, deltaS, gfD, gfU, gfS float64
}

// NewClassifier создает новый классификатор
func NewClassifier(config ClassifierConfig) (*AerosolClassifier, error) {
	if config.Logger == nil {
		logger, _ := zap.NewProduction()
		config.Logger = logger
	}

	if config.NIters <= 0 {
		config.NIters = 1000
	}

	if config.BestSolutions <= 0 {
		config.BestSolutions = 50
	}

	return &AerosolClassifier{
		config: config,
	}, nil
}

// deltaPrime преобразует delta в delta'
func deltaPrime(delta float64) float64 {
	return delta / (1 + delta)
}

// solveSystem решает систему уравнений для одной точки
func (ac *AerosolClassifier) solveSystem(
	params aerosolSolveSystemParams,
) (float64, float64, float64, float64) {

	// Целевая функция для оптимизации
	f := func(x []float64) float64 {
		if len(x) != 3 {
			return math.Inf(1)
		}

		nd, nu, ns := x[0], x[1], x[2]

		// Ограничения: доли должны быть между 0 и 1
		if nd < 0 || nd > 1 || nu < 0 || nu > 1 || ns < 0 || ns > 1 {
			return math.Inf(1)
		}

		eq1 := nd + nu + ns
		eq2 := params.deltaD*nd + params.deltaU*nu + params.deltaS*ns
		eq3 := params.gfD*nd + params.gfU*nu + params.gfS*ns

		// Функционал минимизации
		f1 := math.Pow(eq1-1, 2)
		f2 := math.Pow((eq2-params.deltaPrimeVal)/params.deltaPrimeVal, 2)
		f3 := math.Pow((eq3-params.gfVal)/params.gfVal, 2)

		return f1 + f2 + f3
	}

	// Начальное приближение (равные доли)
	initial := []float64{0.33, 0.33, 0.33}

	// Создаем задачу оптимизации
	problem := optimize.Problem{
		Func: f,
	}

	// Настройки алгоритма Нелдера-Мида
	settings := &optimize.Settings{
		MajorIterations: 1000,
		FuncEvaluations: 2000,
		Converger: &optimize.FunctionConverge{
			Absolute:   1e-6,
			Relative:   1e-6,
			Iterations: 100,
		},
	}

	// Запускаем оптимизацию
	result, err := optimize.Minimize(problem, initial, settings, &optimize.NelderMead{})
	if err != nil {
		return 0, 0, 0, math.Inf(1)
	}

	// Проверяем результат
	if result.F < ac.config.Epsilon {
		return result.X[0], result.X[1], result.X[2], result.F
	}

	return 0, 0, 0, math.Inf(1)
}

// processPoint обрабатывает одну точку данных
func (ac *AerosolClassifier) processPoint(
	deltaVal, gfVal float64,
	wg *sync.WaitGroup,
	resultChan chan<- pointResult,
) {
	defer wg.Done()

	deltaPrimeVal := deltaPrime(deltaVal)

	type solution struct {
		nd, nu, ns             float64
		deltaD, deltaU, deltaS float64
		gfD, gfU, gfS          float64
		residual               float64
	}

	solutions := make([]solution, 0, ac.config.NIters)

	// Генерируем решения
	for _ = range ac.config.NIters {
		// Случайные параметры из диапазонов
		deltaD := randomInRange(ac.config.DustAerosol.DeltaMin, ac.config.DustAerosol.DeltaMax)
		deltaU := randomInRange(ac.config.UrbanAerosol.DeltaMin, ac.config.UrbanAerosol.DeltaMax)
		deltaS := randomInRange(ac.config.SmogAerosol.DeltaMin, ac.config.SmogAerosol.DeltaMax)
		gfD := randomInRange(ac.config.DustAerosol.GFMin, ac.config.DustAerosol.GFMax)
		gfU := randomInRange(ac.config.UrbanAerosol.GFMin, ac.config.UrbanAerosol.GFMax)
		gfS := randomInRange(ac.config.SmogAerosol.GFMin, ac.config.SmogAerosol.GFMax)

		nd, nu, ns, residual := ac.solveSystem(
			aerosolSolveSystemParams{
				deltaPrimeVal: deltaPrimeVal,
				gfVal:         gfVal,
				deltaD:        deltaD,
				deltaU:        deltaU,
				deltaS:        deltaS,
				gfD:           gfD,
				gfU:           gfU,
				gfS:           gfS,
			},
		)

		if residual < math.Inf(1) {
			solutions = append(solutions, solution{
				nd:       nd,
				nu:       nu,
				ns:       ns,
				deltaD:   deltaD,
				deltaU:   deltaU,
				deltaS:   deltaS,
				gfD:      gfD,
				gfU:      gfU,
				gfS:      gfS,
				residual: residual,
			})
		}
	}

	// Сортируем по возрастанию residual
	sort.Slice(solutions, func(i, j int) bool {
		return solutions[i].residual < solutions[j].residual
	})

	// Берем p лучших решений
	p := min(ac.config.BestSolutions, len(solutions))
	if p == 0 {
		resultChan <- pointResult{}
		return
	}

	// Усредняем
	var sumND, sumNU, sumNS float64
	var sumDeltaD, sumDeltaU, sumDeltaS float64
	var sumGFD, sumGFU, sumGFS float64
	var sumResidual float64

	for i := range p {
		sol := solutions[i]
		sumND += sol.nd
		sumNU += sol.nu
		sumNS += sol.ns
		sumDeltaD += sol.deltaD
		sumDeltaU += sol.deltaU
		sumDeltaS += sol.deltaS
		sumGFD += sol.gfD
		sumGFU += sol.gfU
		sumGFS += sol.gfS
		sumResidual += sol.residual
	}

	avgND := sumND / float64(p)
	avgNU := sumNU / float64(p)
	avgNS := sumNS / float64(p)

	resultChan <- pointResult{
		nd:       avgND,
		nu:       avgNU,
		ns:       avgNS,
		deltaD:   sumDeltaD / float64(p),
		deltaU:   sumDeltaU / float64(p),
		deltaS:   sumDeltaS / float64(p),
		gfD:      sumGFD / float64(p),
		gfU:      sumGFU / float64(p),
		gfS:      sumGFS / float64(p),
		residual: sumResidual / float64(p),
	}
}

// pointResult - результат обработки одной точки
type pointResult struct {
	nd, nu, ns             float64
	deltaD, deltaU, deltaS float64
	gfD, gfU, gfS          float64
	residual               float64
}

// solveForS решает систему для S факторов
func solveForS(nd, nu, ns, volBeta *mat.Dense) (float64, float64, float64, *mat.Dense) {
	r, c := nd.Dims()
	n := r * c

	// Создаем матрицу A и вектор b для линейной системы
	A := mat.NewDense(n, 3, nil)
	b := mat.NewVecDense(n, nil)

	idx := 0
	for i := range r {
		for j := range c {
			A.Set(idx, 0, nd.At(i, j))
			A.Set(idx, 1, nu.At(i, j))
			A.Set(idx, 2, ns.At(i, j))
			b.SetVec(idx, volBeta.At(i, j))
			idx++
		}
	}

	// Решаем методом наименьших квадратов с ограничениями
	problem := optimize.Problem{
		Func: func(x []float64) float64 {
			if len(x) != 3 {
				return math.Inf(1)
			}

			// Ограничение неотрицательности
			if x[0] < 0 || x[1] < 0 || x[2] < 0 {
				return math.Inf(1)
			}

			residual := 0.0
			for i := range n {
				pred := x[0]*A.At(i, 0) + x[1]*A.At(i, 1) + x[2]*A.At(i, 2)
				actual := b.AtVec(i)
				residual += math.Pow(pred-actual, 2)
			}

			return residual
		},
	}

	initial := []float64{1.0, 1.0, 1.0}
	settings := &optimize.Settings{
		MajorIterations: 1000,
		FuncEvaluations: 2000,
	}

	result, err := optimize.Minimize(problem, initial, settings, &optimize.NelderMead{})
	if err != nil {
		return 0, 0, 0, nil
	}

	// Вычисляем остатки
	residuals := mat.NewDense(r, c, nil)
	idx = 0
	for i := range r {
		for j := range c {
			pred := result.X[0]*nd.At(i, j) +
				result.X[1]*nu.At(i, j) +
				result.X[2]*ns.At(i, j)
			residuals.Set(i, j, pred-volBeta.At(i, j))
			idx++
		}
	}

	return result.X[0], result.X[1], result.X[2], residuals
}

// Classify выполняет классификацию аэрозолей
func (ac *AerosolClassifier) Classify(dep, flcap, vol, beta *mat.Dense) (
	ClassificationResults,
	error,
) {

	ac.config.Logger.Info("Начало классификации аэрозолей",
		zap.Int("итераций", ac.config.NIters),
		zap.Int("лучших решений", ac.config.BestSolutions))

	// Проверяем размеры матриц
	rows, cols := dep.Dims()
	rf, cf := flcap.Dims()
	rv, cv := vol.Dims()
	rb, cb := beta.Dims()

	if rf != rows || cf != cols || rv != rows || cv != cols || rb != rows || cb != cols {
		return ClassificationResults{}, fmt.Errorf("размеры матриц не совпадают")
	}

	// Инициализируем выходные матрицы
	nd := mat.NewDense(rows, cols, nil)
	nu := mat.NewDense(rows, cols, nil)
	ns := mat.NewDense(rows, cols, nil)
	deltaD := mat.NewDense(rows, cols, nil)
	deltaU := mat.NewDense(rows, cols, nil)
	deltaS := mat.NewDense(rows, cols, nil)
	GFd := mat.NewDense(rows, cols, nil)
	GFu := mat.NewDense(rows, cols, nil)
	GFs := mat.NewDense(rows, cols, nil)
	localResiduals := mat.NewDense(rows, cols, nil)

	// Обрабатываем каждую точку параллельно
	var wg sync.WaitGroup
	resultChan := make(chan pointResult, rows*cols)

	ac.config.Logger.Info("Обработка точек данных",
		zap.Int("всего точек", rows*cols))

	for i := range rows {
		for j := range cols {
			wg.Add(1)
			go ac.processPoint(
				dep.At(i, j),
				flcap.At(i, j),
				&wg,
				resultChan,
			)
		}
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Собираем результаты
	idx := 0
	for result := range resultChan {
		i := idx / cols
		j := idx % cols

		nd.Set(i, j, result.nd)
		nu.Set(i, j, result.nu)
		ns.Set(i, j, result.ns)
		deltaD.Set(i, j, result.deltaD)
		deltaU.Set(i, j, result.deltaU)
		deltaS.Set(i, j, result.deltaS)
		GFd.Set(i, j, result.gfD)
		GFu.Set(i, j, result.gfU)
		GFs.Set(i, j, result.gfS)
		localResiduals.Set(i, j, result.residual)

		idx++

		if idx%1000 == 0 {
			ac.config.Logger.Debug("Обработано точек",
				zap.Int("количество", idx))
		}
	}

	ac.config.Logger.Info("Завершена классификация точек",
		zap.Int("обработано", idx))

	// Вычисляем vol/beta
	volBeta := mat.NewDense(rows, cols, nil)
	if beta != nil {
		// Используем переданный beta
		volBeta.Apply(func(i, j int, v float64) float64 {
			return vol.At(i, j) / beta.At(i, j)
		}, vol)
	} else {
		// Используем vol как есть (beta = 1)
		volBeta.CloneFrom(vol)
	}

	// Решаем систему для S факторов
	ac.config.Logger.Info("Решение системы для S факторов")
	Sd, Su, Ss, residuals := solveForS(nd, nu, ns, volBeta)

	ac.config.Logger.Info("Классификация завершена",
		zap.Float64("Sd", Sd),
		zap.Float64("Su", Su),
		zap.Float64("Ss", Ss))

	return ClassificationResults{
		Residuals: residuals,
		DeltaD:    deltaD,
		DeltaU:    deltaU,
		DeltaS:    deltaS,
		GFd:       GFd,
		GFu:       GFu,
		GFs:       GFs,
		Sd:        Sd,
		Su:        Su,
		Ss:        Ss,
	}, nil
}

// randomInRange возвращает случайное число в заданном диапазоне
func randomInRange(min, max float64) float64 {
	if max <= min {
		return min
	}
	return min + (max-min)*randFloat64()
}

// randFloat64 возвращает случайное число [0, 1)
var randFloat64 = func() float64 {
	// В продакшн коде используйте криптографически безопасный генератор
	return math.Float64frombits(randUint64()>>12) * (1.0 / (1 << 52))
}

var randUint64 = func() uint64 {
	// Заглушка - в реальном коде используйте crypto/rand или math/rand с seed
	return uint64(time.Now().UnixNano())
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
