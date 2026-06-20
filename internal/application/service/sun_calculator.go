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

	hourAngle := s.calculateHourAngle(N)

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

	noon := s.calculateSolarNoon(N)

	sunriseMinutes := noon - ha/15.0*60.0
	sunsetMinutes := noon + ha/15.0*60.0

	sunrise = minutesToTime(year, month, day, sunriseMinutes, s.timezone)
	sunset = minutesToTime(year, month, day, sunsetMinutes, s.timezone)

	_ = hourAngle
	return sunrise, sunset
}

func (s *SunCalculator) calculateSolarNoon(N float64) float64 {
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

func minutesToTime(year int, month time.Month, day int, totalMinutes float64, loc *time.Location) time.Time {
	hours := int(totalMinutes / 60)
	minutes := int(totalMinutes - float64(hours)*60)
	seconds := int((totalMinutes - float64(hours)*60 - float64(minutes)) * 60)

	if hours < 0 {
		hours = 6
	}
	if hours > 23 {
		hours = 18
	}
	if minutes < 0 {
		minutes = 0
	}
	if minutes > 59 {
		minutes = 0
	}
	if seconds < 0 {
		seconds = 0
	}
	if seconds > 59 {
		seconds = 0
	}

	return time.Date(year, month, day, hours, minutes, seconds, 0, loc)
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
