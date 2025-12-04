//! DateTime type for Axmos.
//!
//! Represents a date and time as seconds since Unix epoch.

use crate::types::date::Date;
use crate::{impl_numeric_marker, integer, scalar};
use std::cmp::{Ord, PartialOrd};
use std::fmt::{self, Display};
use std::ops::{Add, AddAssign, Sub, SubAssign};
use std::time::{SystemTime, UNIX_EPOCH};

// Represents a date and time (UTC-based)
integer! {
    pub struct DateTime(u64);
}

// Mark DateTime as numeric
impl_numeric_marker!(DateTime);

// Duration in seconds
scalar! {
    pub struct Seconds(u64);
}

impl Seconds {
    pub const fn new(seconds: u64) -> Self {
        Self(seconds)
    }

    pub const fn value(self) -> u64 {
        self.0
    }
}

/// Time of day (00:00:00 – 23:59:59)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TimeOfDay {
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
}

impl TimeOfDay {
    pub fn new(hour: u8, minute: u8, second: u8) -> Self {
        debug_assert!(
            hour < 24 && minute < 60 && second < 60,
            "Invalid datetime format! {hour}hh-{minute}mm-{second}ss",
        );
        Self {
            hour,
            minute,
            second,
        }
    }

    pub fn as_seconds(self) -> u32 {
        (self.hour as u32 * 3600) + (self.minute as u32 * 60) + self.second as u32
    }

    pub fn from_seconds(seconds: u32) -> Self {
        let hour = (seconds / 3600) as u8;
        let minute = ((seconds % 3600) / 60) as u8;
        let second = (seconds % 60) as u8;
        Self {
            hour,
            minute,
            second,
        }
    }
}

impl Display for TimeOfDay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:02}:{:02}:{:02}", self.hour, self.minute, self.second)
    }
}

impl DateTime {
    /// Unix epoch (1970-01-01T00:00:00Z)
    pub const UNIX_EPOCH: Self = Self(0);

    /// Minimum representable datetime
    pub const MIN: Self = Self(u64::MIN);

    /// Maximum representable datetime
    pub const MAX: Self = Self(u64::MAX);

    /// Create from [`Date`] and [`TimeOfDay`]
    pub fn from_date_and_time(date: Date, time: TimeOfDay) -> Self {
        let seconds = date.days() as u64 * 86_400 + time.as_seconds() as u64;
        Self(seconds)
    }

    /// Extract date portion
    pub fn date(self) -> Date {
        let days = (self.0 / 86_400) as u32;
        Date::from_days(days)
    }

    /// Extract time portion
    pub fn time(self) -> TimeOfDay {
        let seconds_in_day = (self.0 % 86_400) as u32;
        TimeOfDay::from_seconds(seconds_in_day)
    }

    /// Create from components
    pub fn new(year: u32, month: u8, day: u8, hour: u8, minute: u8, second: u8) -> Self {
        let date = Date::new(year, month, day);
        let time = TimeOfDay::new(hour, minute, second);
        Self::from_date_and_time(date, time)
    }

    /// Create from seconds since epoch
    pub const fn from_seconds_since_epoch(seconds: u64) -> Self {
        Self(seconds)
    }

    /// Get seconds since epoch
    pub const fn seconds_since_epoch(self) -> u64 {
        self.0
    }

    /// Current UTC datetime (from system clock)
    pub fn now() -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        Self(now.as_secs())
    }

    /// Parse from ISO 8601: "YYYY-MM-DDTHH:MM:SS"
    pub fn parse_iso(s: &str) -> Result<Self, String> {
        let parts: Vec<&str> = s.split('T').collect();
        if parts.len() != 2 {
            return Err(format!("Invalid datetime format: {s}"));
        }

        let date = Date::parse_iso(parts[0])?;
        let time_parts: Vec<&str> = parts[1].split(':').collect();
        if time_parts.len() != 3 {
            return Err(format!("Invalid time format: {}", parts[1]));
        }

        let hour = time_parts[0].parse::<u8>().map_err(|_| "Invalid hour")?;
        let minute = time_parts[1].parse::<u8>().map_err(|_| "Invalid minute")?;
        let second = time_parts[2].parse::<u8>().map_err(|_| "Invalid second")?;
        let time = TimeOfDay::new(hour, minute, second);
        Ok(Self::from_date_and_time(date, time))
    }

    /// Format as ISO 8601 string
    pub fn to_iso_string(self) -> String {
        format!("{}T{}", self.date().to_iso_string(), self.time())
    }

    /// Add seconds
    pub fn add_seconds(self, seconds: u64) -> Option<Self> {
        self.0
            .checked_add(seconds)
            .map(Self::from_seconds_since_epoch)
    }

    /// Subtract seconds
    pub fn sub_seconds(self, seconds: u64) -> Option<Self> {
        self.0
            .checked_sub(seconds)
            .map(Self::from_seconds_since_epoch)
    }

    /// Add days
    pub fn add_days(self, days: i32) -> Option<Self> {
        self.add_seconds(days as u64 * 86_400)
    }

    /// Difference in seconds
    pub const fn seconds_between(self, other: Self) -> u64 {
        other.0 - self.0
    }

    /// Difference in whole days
    pub const fn days_between(self, other: Self) -> i32 {
        (self.seconds_between(other) / 86_400) as i32
    }
}

// Arithmetic
impl Add<Seconds> for DateTime {
    type Output = DateTime;
    fn add(self, s: Seconds) -> Self::Output {
        self.add_seconds(s.0).expect("DateTime overflow")
    }
}

impl AddAssign<Seconds> for DateTime {
    fn add_assign(&mut self, s: Seconds) {
        *self = *self + s;
    }
}

impl Sub<Seconds> for DateTime {
    type Output = DateTime;
    fn sub(self, s: Seconds) -> Self::Output {
        self.sub_seconds(s.0).expect("DateTime underflow")
    }
}

impl SubAssign<Seconds> for DateTime {
    fn sub_assign(&mut self, s: Seconds) {
        *self = *self - s;
    }
}

// Conversions
impl From<(Date, TimeOfDay)> for DateTime {
    fn from((date, time): (Date, TimeOfDay)) -> Self {
        Self::from_date_and_time(date, time)
    }
}

impl TryFrom<&str> for DateTime {
    type Error = String;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::parse_iso(value)
    }
}

impl From<DateTime> for String {
    fn from(dt: DateTime) -> Self {
        dt.to_iso_string()
    }
}
