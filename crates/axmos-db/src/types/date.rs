//! Date type for Axmos.
//!
//! Represents a date as days since Unix epoch (1970-01-01).

use crate::{
    datetime::TimeOfDay,
    types::{
        Blob, DateTime,
        core::AxmosCastable,
        sized_types::{Float32, Float64, Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64},
    },
};
use crate::{direct_axmos_cast, unsigned_integer};
use std::cmp::{Ord, PartialOrd};
use std::ops::{Add, AddAssign, Sub, SubAssign};
use std::time::{SystemTime, UNIX_EPOCH};

const MONTH_DAYS: [u16; 13] = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

/// Check if this year is a leap year
pub(crate) const fn is_leap_year(year: u32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

// Helper to convert year-month-day to days since epoch
pub(crate) const fn ymd_to_days(year: u32, month: u8, day: u8) -> u32 {
    let y = year as i32;
    let m = month as i32;
    let d = day as i32;

    let adjusted_m = if m <= 2 { m + 9 } else { m - 3 };
    let adjusted_y = if m <= 2 { y - 1 } else { y };

    let era = adjusted_y / 400;
    let yoe = adjusted_y - era * 400;
    let doy = (153 * adjusted_m + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;

    let total_days = era * 146097 + doe - 719468;
    (total_days - 719162) as u32
}

// Days since Unix epoch
unsigned_integer! {
    pub struct Date(u32);
}

// Duration in days
pub struct Days(u32);

impl Days {
    pub const fn new(days: u32) -> Self {
        Self(days)
    }

    pub const fn value(self) -> u32 {
        self.0
    }
}

/// Month of the year
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum Month {
    January = 1,
    February = 2,
    March = 3,
    April = 4,
    May = 5,
    June = 6,
    July = 7,
    August = 8,
    September = 9,
    October = 10,
    November = 11,
    December = 12,
}

impl Month {
    pub fn from_repr(value: u8) -> Result<Self, ()> {
        match value {
            1 => Ok(Month::January),
            2 => Ok(Month::February),
            3 => Ok(Month::March),
            4 => Ok(Month::April),
            5 => Ok(Month::May),
            6 => Ok(Month::June),
            7 => Ok(Month::July),
            8 => Ok(Month::August),
            9 => Ok(Month::September),
            10 => Ok(Month::October),
            11 => Ok(Month::November),
            12 => Ok(Month::December),
            _ => Err(()),
        }
    }

    pub const fn as_repr(self) -> u8 {
        self as u8
    }

    pub fn from_u8(value: u8) -> Option<Self> {
        Self::from_repr(value).ok()
    }

    pub const fn as_u8(self) -> u8 {
        self.as_repr()
    }

    pub fn name(&self) -> &'static str {
        match self {
            Month::January => "January",
            Month::February => "February",
            Month::March => "March",
            Month::April => "April",
            Month::May => "May",
            Month::June => "June",
            Month::July => "July",
            Month::August => "August",
            Month::September => "September",
            Month::October => "October",
            Month::November => "November",
            Month::December => "December",
        }
    }

    fn days_in_month(&self, year: u32) -> u8 {
        match self {
            Self::January => 31,
            Self::February => {
                if is_leap_year(year) {
                    29
                } else {
                    28
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

/// Day of the week
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum Weekday {
    Monday = 1,
    Tuesday = 2,
    Wednesday = 3,
    Thursday = 4,
    Friday = 5,
    Saturday = 6,
    Sunday = 7,
}

impl Weekday {
    pub fn from_repr(value: u8) -> Result<Self, ()> {
        match value {
            1 => Ok(Weekday::Monday),
            2 => Ok(Weekday::Tuesday),
            3 => Ok(Weekday::Wednesday),
            4 => Ok(Weekday::Thursday),
            5 => Ok(Weekday::Friday),
            6 => Ok(Weekday::Saturday),
            7 => Ok(Weekday::Sunday),
            _ => Err(()),
        }
    }

    pub const fn as_repr(self) -> u8 {
        self as u8
    }

    pub fn from_u8(value: u8) -> Option<Self> {
        Self::from_repr(value).ok()
    }

    pub const fn as_u8(self) -> u8 {
        self.as_repr()
    }

    fn is_weekend(&self) -> bool {
        matches!(self, Weekday::Saturday | Weekday::Sunday)
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
    pub fn new(year: u32, month: u8, day: u8) -> Self {
        debug_assert!(
            year > 0 && month > 0 && month <= 12 && day > 0 && day <= 31,
            "Invalid date format {year}-{month}-{day}"
        );

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
    pub fn from_days(days: u32) -> Self {
        Self(days)
    }

    /// Get the number of days since Unix epoch
    pub fn days(self) -> u32 {
        self.0
    }

    /// Create today's date
    pub fn today() -> Self {
        let today = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");

        Self(today.as_secs().div_ceil(3600).div_ceil(24) as u32)
    }

    /// Parse from ISO 8601 format (YYYY-MM-DD)
    pub fn parse_iso(s: &str) -> Result<Self, String> {
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
    pub fn to_iso_string(self) -> String {
        let (year, month, day) = self.yyyy_mm_dd();
        format!("{year:04}-{month:02}-{day:02}")
    }

    /// Get the year
    pub fn year(self) -> u32 {
        self.yyyy_mm_dd().0
    }

    /// Get the month as enum
    pub fn month(self) -> Month {
        Month::from_u8(self.yyyy_mm_dd().1).unwrap()
    }

    /// Get the day of the month
    pub fn day(self) -> u8 {
        self.yyyy_mm_dd().2
    }

    /// Convert days since epoch back into (year, month, day)
    pub fn yyyy_mm_dd(self) -> (u32, u8, u8) {
        let days = self.0 as i32 + 719_468;
        let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
        let doe = days - era * 146_097;
        let yoe = (4000 * (doe + 1)) / 1_461_001;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let day = doy - (153 * mp + 2) / 5 + 1;
        let month = mp + if mp < 10 { 3 } else { -9 };
        let year = era * 400 + yoe + if month <= 2 { 1 } else { 0 };
        (year as u32, month as u8, day as u8)
    }

    /// Calculate day of the week (Monday = 1 … Sunday = 7)
    pub fn weekday(self) -> Weekday {
        let weekday_num = ((self.0 + 4) % 7) + 1;
        Weekday::from_u8(weekday_num as u8).unwrap()
    }

    pub fn day_of_year(self) -> u16 {
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
        let day_of_year = self.day_of_year();
        let weekday = self.weekday() as u8;
        let week = (day_of_year + 10 - weekday as u16) / 7;
        week.clamp(1, 53) as u8
    }

    /// Check if this date's year is a leap year
    pub fn is_leap_year(self) -> bool {
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

direct_axmos_cast!(
    Date[u32]  => Int8[i8], Int16[i16], Int32[i32], UInt8[u8], UInt16[u16], UInt32[u32], Int64[i64], Float32[f32], Float64[f64], UInt64[u64]
);

impl AxmosCastable<Blob> for Date {
    fn can_cast(&self) -> bool {
        true
    }

    fn try_cast(&self) -> Option<Blob> {
        Some(Blob::from(self.to_iso_string().as_str()))
    }
}

impl AxmosCastable<DateTime> for Date {
    fn can_cast(&self) -> bool {
        true
    }

    fn try_cast(&self) -> Option<DateTime> {
        Some(DateTime::from_date_and_time(
            self.clone(),
            TimeOfDay::from_seconds(0),
        ))
    }
}
