use crate::serialization::Serializable;
use crate::types::{DataType, DataTypeMarker};
use std::cmp::{Ord, Ordering, PartialOrd};
use std::fmt::{self, Display};
use std::ops::{Add, AddAssign, Sub, SubAssign};
use std::time::{SystemTime, UNIX_EPOCH};

/// Days since Unix epoch (1970-01-01)
/// This allows us to represent dates from approximately year -5877641 to year 5881580
/// Using u32 for storage efficiency (4 bytes)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Date {
    days_since_epoch: u32,
}

/// Duration in days
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Days(pub u32);

impl Days {
    pub const fn new(days: u32) -> Self {
        Self(days)
    }

    pub const fn value(self) -> u32 {
        self.0
    }
}

/// Represents a specific month of the year
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
    pub fn from_u8(month: u8) -> Option<Self> {
        match month {
            1 => Some(Month::January),
            2 => Some(Month::February),
            3 => Some(Month::March),
            4 => Some(Month::April),
            5 => Some(Month::May),
            6 => Some(Month::June),
            7 => Some(Month::July),
            8 => Some(Month::August),
            9 => Some(Month::September),
            10 => Some(Month::October),
            11 => Some(Month::November),
            12 => Some(Month::December),
            _ => None,
        }
    }

    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    pub const fn days_in_month(self, year: u32) -> u8 {
        match self {
            Month::January => 31,
            Month::February => {
                if Date::is_leap_year(year) {
                    29
                } else {
                    28
                }
            }
            Month::March => 31,
            Month::April => 30,
            Month::May => 31,
            Month::June => 30,
            Month::July => 31,
            Month::August => 31,
            Month::September => 30,
            Month::October => 31,
            Month::November => 30,
            Month::December => 31,
        }
    }

    pub const fn name(self) -> &'static str {
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

    pub const fn short_name(self) -> &'static str {
        match self {
            Month::January => "Jan",
            Month::February => "Feb",
            Month::March => "Mar",
            Month::April => "Apr",
            Month::May => "May",
            Month::June => "Jun",
            Month::July => "Jul",
            Month::August => "Aug",
            Month::September => "Sep",
            Month::October => "Oct",
            Month::November => "Nov",
            Month::December => "Dec",
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
    pub const fn name(self) -> &'static str {
        match self {
            Weekday::Monday => "Monday",
            Weekday::Tuesday => "Tuesday",
            Weekday::Wednesday => "Wednesday",
            Weekday::Thursday => "Thursday",
            Weekday::Friday => "Friday",
            Weekday::Saturday => "Saturday",
            Weekday::Sunday => "Sunday",
        }
    }

    pub const fn short_name(self) -> &'static str {
        match self {
            Weekday::Monday => "Mon",
            Weekday::Tuesday => "Tue",
            Weekday::Wednesday => "Wed",
            Weekday::Thursday => "Thu",
            Weekday::Friday => "Fri",
            Weekday::Saturday => "Sat",
            Weekday::Sunday => "Sun",
        }
    }

    pub const fn is_weekend(self) -> bool {
        matches!(self, Weekday::Saturday | Weekday::Sunday)
    }
}

impl Date {
    /// Unix epoch (1970-01-01)
    pub const UNIX_EPOCH: Self = Self {
        days_since_epoch: 0,
    };

    /// Minimum representable date
    pub const MIN: Self = Self {
        days_since_epoch: u32::MIN,
    };

    /// Maximum representable date
    pub const MAX: Self = Self {
        days_since_epoch: u32::MAX,
    };

    /// Create a new Date from year, month, and day
    pub const fn new(year: u32, month: u8, day: u8) -> Option<Self> {
        if month < 1 || month > 12 || day < 1 {
            return None;
        }

        // Validate day is within month bounds
        let days_in_month = match month {
            1 => 31,
            2 => {
                if Self::is_leap_year(year) {
                    29
                } else {
                    28
                }
            }
            3 => 31,
            4 => 30,
            5 => 31,
            6 => 30,
            7 => 31,
            8 => 31,
            9 => 30,
            10 => 31,
            11 => 30,
            12 => 31,
            _ => return None,
        };

        if day > days_in_month {
            return None;
        }

        // Calculate days since epoch
        let days = Self::ymd_to_days_since_epoch(year, month, day);
        Some(Self {
            days_since_epoch: days,
        })
    }

    /// Create a Date from days since Unix epoch
    pub const fn from_days_since_epoch(days: u32) -> Self {
        Self {
            days_since_epoch: days,
        }
    }

    /// Get the number of days since Unix epoch
    pub const fn days_since_epoch(self) -> u32 {
        self.days_since_epoch
    }

    /// Create today's date
    pub fn today() -> Self {
        let today = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");

        Self {
            days_since_epoch: today.as_secs().div_ceil(3600).div_ceil(24) as u32,
        }
    }

    /// Parse from ISO 8601 format (YYYY-MM-DD)
    pub fn parse_iso(s: &str) -> Result<Self, String> {
        let parts: Vec<&str> = s.split('-').collect();
        if parts.len() != 3 {
            return Err(format!("Invalid date format: {}", s));
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

        Self::new(year, month, day)
            .ok_or_else(|| format!("Invalid date: {}-{}-{}", year, month, day))
    }

    /// Format as ISO 8601 (YYYY-MM-DD)
    pub fn to_iso_string(self) -> String {
        let (year, month, day) = self.year_month_day();
        format!("{:04}-{:02}-{:02}", year, month, day)
    }

    /// Get the year
    pub const fn year(self) -> u32 {
        self.year_month_day().0
    }

    /// Get the month
    pub const fn month(self) -> u8 {
        self.year_month_day().1
    }

    /// Get the month as enum
    pub fn month_enum(self) -> Month {
        Month::from_u8(self.month()).unwrap()
    }

    /// Get the day of the month
    pub const fn day(self) -> u8 {
        self.year_month_day().2
    }

    /// Get the day of the week
    pub const fn weekday(self) -> Weekday {
        // Zeller's congruence algorithm for Gregorian calendar
        let (year, month, day) = self.year_month_day();

        let m = if month <= 2 { month + 12 } else { month } as u32;
        let y = if month <= 2 { year - 1 } else { year };

        let k = y % 100;
        let j = y / 100;

        let h = (day as u32 + 13 * (m + 1) / 5 + k + k / 4 + j / 4 - 2 * j) % 7;

        // Convert to Monday = 1, Sunday = 7
        match h {
            0 => Weekday::Saturday,
            1 => Weekday::Sunday,
            2 => Weekday::Monday,
            3 => Weekday::Tuesday,
            4 => Weekday::Wednesday,
            5 => Weekday::Thursday,
            6 => Weekday::Friday,
            _ => Weekday::Monday, // Should never happen
        }
    }

    /// Get the day of the year (1-366)
    pub const fn day_of_year(self) -> u16 {
        let (year, month, day) = self.year_month_day();
        let mut days = day as u16;

        let month_days = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
        let mut i = 1;
        while i < month {
            days += month_days[i as usize];
            if i == 2 && Self::is_leap_year(year) {
                days += 1;
            }
            i += 1;
        }

        days
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

    /// Check if this year is a leap year
    pub const fn is_leap_year(year: u32) -> bool {
        (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
    }

    /// Check if this date's year is a leap year
    pub const fn is_in_leap_year(self) -> bool {
        Self::is_leap_year(self.year())
    }

    /// Add days to the date
    pub const fn add_days(self, days: u32) -> Option<Self> {
        match self.days_since_epoch.checked_add(days) {
            Some(result) => Some(Self {
                days_since_epoch: result,
            }),
            None => None,
        }
    }

    /// Subtract days from the date
    pub const fn sub_days(self, days: u32) -> Option<Self> {
        match self.days_since_epoch.checked_sub(days) {
            Some(result) => Some(Self {
                days_since_epoch: result,
            }),
            None => None,
        }
    }

    /// Add months to the date (handles month-end edge cases)
    pub fn add_months(self, months: u32) -> Self {
        let (year, month, day) = self.year_month_day();

        let total_months = year * 12 + month as u32 - 1 + months;
        let new_year = total_months.div_euclid(12);
        let new_month = (total_months.rem_euclid(12) + 1) as u8;

        // Handle day overflow (e.g., Jan 31 + 1 month = Feb 28/29)
        let max_day = Month::from_u8(new_month).unwrap().days_in_month(new_year);
        let new_day = day.min(max_day);

        Self::new(new_year, new_month, new_day).unwrap()
    }

    /// Add years to the date
    pub fn add_years(self, years: u32) -> Self {
        let (year, month, day) = self.year_month_day();
        let new_year = year + years;

        // Handle Feb 29 in non-leap years
        let new_day = if month == 2 && day == 29 && !Self::is_leap_year(new_year) {
            28
        } else {
            day
        };

        Self::new(new_year, month, new_day).unwrap()
    }

    /// Get the difference in days between two dates
    pub const fn days_between(self, other: Self) -> u32 {
        other.days_since_epoch - self.days_since_epoch
    }

    /// Get the first day of the month
    pub const fn first_of_month(self) -> Self {
        let (year, month, _) = self.year_month_day();
        Self::new(year, month, 1).unwrap()
    }

    /// Get the last day of the month
    pub fn last_of_month(self) -> Self {
        let (year, month, _) = self.year_month_day();
        let last_day = Month::from_u8(month).unwrap().days_in_month(year);
        Self::new(year, month, last_day).unwrap()
    }

    /// Get the first day of the year
    pub const fn first_of_year(self) -> Self {
        let (year, _, _) = self.year_month_day();
        Self::new(year, 1, 1).unwrap()
    }

    /// Get the last day of the year
    pub const fn last_of_year(self) -> Self {
        let (year, _, _) = self.year_month_day();
        Self::new(year, 12, 31).unwrap()
    }

    /// Check if this date is a weekend
    pub const fn is_weekend(self) -> bool {
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

    /// Get year, month, and day components
    pub const fn year_month_day(self) -> (u32, u8, u8) {
        Self::days_to_ymd(self.days_since_epoch)
    }

    // Helper: Convert year-month-day to days since epoch
    const fn ymd_to_days_since_epoch(year: u32, month: u8, day: u8) -> u32 {
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


    // Helper: Convert days since epoch to year-month-day
    const fn days_to_ymd(days_since_epoch: u32) -> (u32, u8, u8) {
        // This is a simplified inverse calculation
        // In production, use a proper date library

        // Approximate year
        let year_approx = 1970 + days_since_epoch / 365;

        // Refine and find exact date
        // This is a placeholder - real implementation would be more complex
        let mut year = year_approx;
        let mut remaining = days_since_epoch - Self::ymd_to_days_since_epoch(year, 1, 1);

        while remaining >= 365 + Self::is_leap_year(year) as u32 {
            remaining -= 365 + Self::is_leap_year(year) as u32;
            year += 1;
        }

        // Find month and day
        let month_days = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
        let mut month = 1u8;
        let mut day_count = 0;

        while month <= 12 {
            let days_in_month = if month == 2 && Self::is_leap_year(year) {
                29
            } else {
                month_days[month as usize]
            };

            if remaining < day_count + days_in_month {
                break;
            }

            day_count += days_in_month;
            month += 1;
        }

        let day = (remaining - day_count + 1) as u8;

        (year, month, day)
    }
}

impl DataType for Date {
    fn _type_of(&self) -> DataTypeMarker {
        DataTypeMarker::Date
    }

    fn size_of(&self) -> u16 {
        4 // 4 bytes for u32
    }
}

impl Display for Date {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_iso_string())
    }
}

impl Serializable for Date {
    fn read_from<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let mut bytes = [0u8; 4];
        reader.read_exact(&mut bytes)?;
        let days = u32::from_be_bytes(bytes);
        Ok(Date::from_days_since_epoch(days))
    }

    fn write_to<W: std::io::Write>(self, writer: &mut W) -> std::io::Result<()> {
        let bytes = self.days_since_epoch.to_be_bytes();
        writer.write_all(&bytes)?;
        Ok(())
    }
}

impl PartialOrd for Date {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Date {
    fn cmp(&self, other: &Self) -> Ordering {
        self.days_since_epoch.cmp(&other.days_since_epoch)
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

impl Sub<Date> for Date {
    type Output = Days;

    fn sub(self, other: Date) -> Self::Output {
        Days(self.days_between(other))
    }
}

// Conversions
impl From<(u32, u8, u8)> for Date {
    fn from((year, month, day): (u32, u8, u8)) -> Self {
        Self::new(year, month, day).expect("Invalid date")
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
