//! Implementation of the date type was moved here to not put so much boilerplate code on the sized_types module.
use crate::repr_enum;

use std::cmp::{Ord, PartialOrd};
use std::ops::{Add, AddAssign, Sub, SubAssign};
use std::time::{SystemTime, UNIX_EPOCH};

const MONTH_DAYS: [u16; 13] = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
/// Check if this year is a leap year
pub(crate) const fn is_leap_year(year: u32) -> bool {
    (year.is_multiple_of(4) && year.is_multiple_of(100)) || (year.is_multiple_of(400))
}

// Helper to convert year-month-day to days since epoch
pub(crate) const fn ymd_to_days(year: u32, month: u8, day: u8) -> u32 {
    // Algorithm based on the Gregorian calendar
    let y = year as i32;
    let m = month as i32;
    let d = day as i32;

    // Adjust months so March is month 0, January and February are months 10 and 11
    let adjusted_m = if m <= 2 { m + 9 } else { m - 3 };
    let adjusted_y = if m <= 2 { y - 1 } else { y };

    // Calculate days using the formula
    let era = adjusted_y / 400;
    let yoe = adjusted_y - era * 400; // [0, 399]
    let doy = (153 * adjusted_m + 2) / 5 + d - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]

    let total_days = era * 146097 + doe - 719468;

    // Convert to days since Unix epoch (1970-01-01 = 719162 days from 0000-03-01)
    (total_days - 719162) as u32
}

// Days since Unix epoch (1970-01-01)
// This allows us to represent dates from approximately year -5877641 to year 5881580
// Using u32 for storage efficiency (4 bytes)
crate::integer! {
    pub struct Date(u32);
}

// Duration in days
crate::scalar! {
    pub struct Days(u32);
}

impl Days {
    pub const fn new(days: u32) -> Self {
        Self(days)
    }

    pub const fn value(self) -> u32 {
        self.0
    }
}

repr_enum! {
    /// Month of the year
    pub enum Month {
        January = 1 => ("January", "Jan"),
        February = 2 => ("February", "Feb"),
        March = 3 => ("March", "Mar"),
        April = 4 => ("April", "Apr"),
        May = 5 => ("May", "May"),
        June = 6 => ("June", "Jun"),
        July = 7 => ("July", "Jul"),
        August = 8 => ("August", "Aug"),
        September = 9 => ("September", "Sep"),
        October = 10 => ("October", "Oct"),
        November = 11 => ("November", "Nov"),
        December = 12 => ("December", "Dec"),
    }
}

impl Month {
    fn days_in_month(&self, year: u32) -> u8 {
        match self {
            Self::January => 31,
            Self::February => {
                if !is_leap_year(year) {
                    28
                } else {
                    29
                }
            }
            Self::March => 31,
            Self::April => 30,
            Self::May => 31,
            Self::June => 30,
            Self::July => 31,
            Self::August => 31,
            Self::September => 30,
            Self::October => 31,
            Self::November => 30,
            Self::December => 31,
        }
    }
}

repr_enum! {
    /// Month of the year
    pub enum Weekday {
        Monday = 1 => ("Monday", "Mon"),
        Tuesday = 2 => ("Tuesday", "Tue"),
        Wednesday = 3 => ("Wednesday", "Wed"),
        Thursday = 4 => ("Thursday", "Thu"),
        Friday = 5 => ("Friday", "Fri"),
        Saturday = 6 => ("Saturday", "Sat"),
        Sunday = 7 => ("Sunday", "Sun"),
    }
}

impl Month {
    pub fn from_u8(value: u8) -> Option<Self> {
        Month::from_repr(value).ok()
    }

    pub const fn as_u8(self) -> u8 {
        self.as_repr()
    }
}

impl Weekday {
    pub fn from_u8(value: u8) -> Option<Self> {
        Weekday::from_repr(value).ok()
    }

    pub const fn as_u8(self) -> u8 {
        self.as_repr()
    }

    fn is_weekend(&self) -> bool {
        matches!(self, Weekday::Saturday) || matches!(self, Weekday::Sunday)
    }
}

impl Date {
    /// Unix epoch (1970-01-01)
    pub const UNIX_EPOCH: Self = Self(0);

    /// Minimum representable date
    pub const MIN: Self = Self(u32::MIN);

    /// Maximum representable date
    pub const MAX: Self = Self(u32::MAX);

    /// Create a new Date from year, month, and day
    pub(crate) fn new(year: u32, month: u8, day: u8) -> Self {
        debug_assert!(
            year > 0 && month > 0 && month <= 12 && day > 0 && day <= 31,
            "Invalid date format {year}-{month}-{day}"
        );

        // Validate day is within month bounds
        let month_value = Month::from_u8(month).unwrap();
        debug_assert!(
            day <= month_value.days_in_month(year),
            "Invalid day value for month {}.Max value is {}, but received: {}",
            month_value.name(),
            month_value.days_in_month(year),
            day
        );

        Self(ymd_to_days(year, month, day))
    }

    /// Create a Date from days since Unix epoch
    pub(crate) fn from_days(days: u32) -> Self {
        Self(days)
    }

    /// Get the number of days since Unix epoch
    pub(crate) fn days(self) -> u32 {
        self.0
    }

    /// Create today's date
    pub(crate) fn today() -> Self {
        let today = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");

        Self(today.as_secs().div_ceil(3600).div_ceil(24) as u32)
    }

    /// Parse from ISO 8601 format (YYYY-MM-DD)
    pub(crate) fn parse_iso(s: &str) -> Result<Self, String> {
        let parts: Vec<&str> = s.split('-').collect();
        if parts.len() != 3 {
            return Err(format!("Invalid date format: {s}"));
        }

        let year = parts[0]
            .parse::<u32>()
            .map_err(|_| format!("Invalid year: {}", parts[0]))?;
        let month = parts[1]
            .parse::<u8>()
            .map_err(|_| format!("Invalid month: {}", parts[1]))?;
        let day = parts[2]
            .parse::<u8>()
            .map_err(|_| format!("Invalid day: {}", parts[2]))?;

        Ok(Self::new(year, month, day))
    }

    /// Format as ISO 8601 (YYYY-MM-DD)
    pub(crate) fn to_iso_string(self) -> String {
        let (year, month, day) = self.yyyy_mm_dd();
        format!("{year:04}-{month:02}-{day:02}")
    }

    /// Get the year
    pub(crate) fn year(self) -> u32 {
        self.yyyy_mm_dd().0
    }

    /// Get the month as enum
    pub fn month(self) -> Month {
        Month::from_u8(self.yyyy_mm_dd().1).unwrap()
    }

    /// Get the day of the month
    pub(crate) fn day(self) -> u8 {
        self.yyyy_mm_dd().2
    }

    /// Convert days since epoch back into (year, month, day)
    pub(crate) fn yyyy_mm_dd(self) -> (u32, u8, u8) {
        // Algorithm adapted from Howard Hinnant’s date algorithms
        // https://howardhinnant.github.io/date/date.html
        let days = self.0 as i32 + 719_468;
        let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
        let doe = days - era * 146_097; // [0, 146096]
        let yoe = (4000 * (doe + 1)) / 1_461_001; // [0, 399]
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
        let mp = (5 * doy + 2) / 153; // [0, 11]
        let day = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
        let month = mp + if mp < 10 { 3 } else { -9 }; // [1, 12]
        let year = era * 400 + yoe + if month <= 2 { 1 } else { 0 };
        (year as u32, month as u8, day as u8)
    }

    /// Calculate day of the week (Monday = 1 … Sunday = 7)
    pub(crate) fn weekday(self) -> Weekday {
        // 1970-01-01 was a Thursday
        let weekday_num = ((self.0 + 4) % 7) + 1;
        Weekday::from_u8(weekday_num as u8).unwrap()
    }

    pub(crate) fn day_of_year(self) -> u16 {
        let (year, month, day) = self.yyyy_mm_dd();
        let month_days = MONTH_DAYS;
        let mut total = day as u16;
        let mut i = 1;
        while i < month {
            total += month_days[i as usize];
            if i == 2 && is_leap_year(year) {
                total += 1;
            }
            i += 1;
        }
        total
    }

    /// Get the ISO week number (1-53)
    pub fn iso_week(self) -> u8 {
        // This is a simplified version. Full ISO week calculation is complex
        let day_of_year = self.day_of_year();
        let weekday = self.weekday() as u8;

        // Adjust for weeks starting on Monday
        let week = (day_of_year + 10 - weekday as u16) / 7;
        week.clamp(1, 53) as u8
    }

    /// Check if this date's year is a leap year
    pub(crate) fn is_leap_year(self) -> bool {
        is_leap_year(self.year())
    }

    /// Add days to the date
    pub const fn add_days(self, days: u32) -> Option<Self> {
        match self.0.checked_add(days) {
            Some(result) => Some(Self(result)),
            None => None,
        }
    }

    /// Subtract days from the date
    pub const fn sub_days(self, days: u32) -> Option<Self> {
        match self.0.checked_sub(days) {
            Some(result) => Some(Self(result)),
            None => None,
        }
    }

    /// Get the difference in days between two dates
    pub const fn days_between(self, other: Self) -> u32 {
        other.0 - self.0
    }

    /// Check if this date is a weekend
    pub fn is_weekend(self) -> bool {
        self.weekday().is_weekend()
    }

    /// Get next business day (skips weekends)
    pub fn next_business_day(self) -> Self {
        let next = self.add_days(1).unwrap();
        if next.is_weekend() {
            if next.weekday() == Weekday::Saturday {
                next.add_days(2).unwrap()
            } else {
                next.add_days(1).unwrap()
            }
        } else {
            next
        }
    }
}

// Arithmetic operations
impl Add<Days> for Date {
    type Output = Date;

    fn add(self, days: Days) -> Self::Output {
        self.add_days(days.0).expect("Date overflow")
    }
}

impl AddAssign<Days> for Date {
    fn add_assign(&mut self, days: Days) {
        *self = *self + days;
    }
}

impl Sub<Days> for Date {
    type Output = Date;

    fn sub(self, days: Days) -> Self::Output {
        self.sub_days(days.0).expect("Date underflow")
    }
}

impl SubAssign<Days> for Date {
    fn sub_assign(&mut self, days: Days) {
        *self = *self - days;
    }
}

// Conversions
impl From<(u32, u8, u8)> for Date {
    fn from((year, month, day): (u32, u8, u8)) -> Self {
        Self::new(year, month, day)
    }
}

impl TryFrom<&str> for Date {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::parse_iso(value)
    }
}

impl From<Date> for String {
    fn from(date: Date) -> Self {
        date.to_iso_string()
    }
}
