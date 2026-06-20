package service

import (
	"math"
	"time"
)

type SunCalculator struct {
	latitude  float64
	longitude float64
	timezone  *time.Location
}

func NewSunCalculator(latitude, longitude float64, timezone *time.Location) *SunCalculator {
	if timezone == nil {
		timezone = time.Local
	}
	return &SunCalculator{
		latitude:  latitude,
		longitude: longitude,
		timezone:  timezone,
	}
}

func (s *SunCalculator) toRadians(degrees float64) float64 {
	return degrees * math.Pi / 180.0
}

func (s *SunCalculator) toDegrees(radians float64) float64 {
	return radians * 180.0 / math.Pi
}

func (s *SunCalculator) CalculateSunTimes(date time.Time) (sunrise, sunset time.Time) {
	localDate := date.In(s.timezone)
	year, month, day := localDate.Date()

	N := float64(localDate.YearDay())

	zenith := 90.833

	declination := 23.45 * math.Sin(s.toRadians(360.0/365.0*(284.0+N)))

	latRad := s.toRadians(s.latitude)
	decRad := s.toRadians(declination)

	ha := s.toDegrees(math.Acos(
		math.Cos(s.toRadians(zenith))/
			(math.Cos(latRad)*math.Cos(decRad)) -
			math.Tan(latRad)*math.Tan(decRad)))

	if math.IsNaN(ha) {
		sunrise = time.Date(year, month, day, 6, 0, 0, 0, s.timezone)
		sunset = time.Date(year, month, day, 18, 0, 0, 0, s.timezone)
		return sunrise, sunset
	}

	noonUTC := s.calculateSolarNoonUTC(N)

	sunriseMinutesUTC := noonUTC - ha/15.0*60.0
	sunsetMinutesUTC := noonUTC + ha/15.0*60.0

	sunriseUTC := minutesToTimeUTC(year, month, day, sunriseMinutesUTC)
	sunsetUTC := minutesToTimeUTC(year, month, day, sunsetMinutesUTC)

	sunrise = sunriseUTC.In(s.timezone)
	sunset = sunsetUTC.In(s.timezone)

	return sunrise, sunset
}

func (s *SunCalculator) calculateSolarNoonUTC(N float64) float64 {
	tGamma := (2.0 * math.Pi / 365.0) * (N - 1.0)

	eqTime := 229.18 * (0.000075 +
		0.001868*math.Cos(tGamma) -
		0.032077*math.Sin(tGamma) -
		0.014615*math.Cos(2.0*tGamma) -
		0.040849*math.Sin(2.0*tGamma))

	timeOffset := eqTime + 4.0*s.longitude

	return 720.0 - timeOffset
}

func (s *SunCalculator) calculateHourAngle(N float64) float64 {
	return 0
}

func minutesToTimeUTC(year int, month time.Month, day int, totalMinutes float64) time.Time {
	base := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	duration := time.Duration(totalMinutes * float64(time.Minute))
	return base.Add(duration)
}

func (s *SunCalculator) IsDaylight(now time.Time) bool {
	sunrise, sunset := s.CalculateSunTimes(now)
	return !now.Before(sunrise) && !now.After(sunset)
}

func (s *SunCalculator) GetDaylightDuration(date time.Time) time.Duration {
	sunrise, sunset := s.CalculateSunTimes(date)
	return sunset.Sub(sunrise)
}

func (s *SunCalculator) GetTimeUntilSunset(now time.Time) time.Duration {
	_, sunset := s.CalculateSunTimes(now)
	if now.After(sunset) {
		return 0
	}
	return sunset.Sub(now)
}

func (s *SunCalculator) GetTimeSinceSunrise(now time.Time) time.Duration {
	sunrise, _ := s.CalculateSunTimes(now)
	if now.Before(sunrise) {
		return 0
	}
	return now.Sub(sunrise)
}
