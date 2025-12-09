#[macro_export]
macro_rules! vector {
    ($name:ident, $inner:ty) => {
        #[derive(Debug, Clone)]
        pub(crate) struct $name(Vec<$inner>);

        impl $name {
            pub fn new() -> Self {
                Self(Vec::new())
            }

            pub fn with_capacity(capacity: usize) -> Self {
                Self(Vec::with_capacity(capacity))
            }

            pub fn len(&self) -> usize {
                self.0.len()
            }

            pub fn is_empty(&self) -> bool {
                self.0.is_empty()
            }

            pub fn push(&mut self, value: $inner) {
                self.0.push(value);
            }

            pub fn pop(&mut self) -> Option<$inner> {
                self.0.pop()
            }

            pub fn clear(&mut self) {
                self.0.clear();
            }

            pub fn inner(&self) -> &Vec<$inner> {
                &self.0
            }

            pub fn inner_mut(&mut self) -> &mut Vec<$inner> {
                &mut self.0
            }

            pub fn into_inner(self) -> Vec<$inner> {
                self.0
            }

            pub fn reserve(&mut self, additional: usize) {
                self.0.reserve(additional);
            }

            pub fn shrink_to_fit(&mut self) {
                self.0.shrink_to_fit();
            }

            pub fn capacity(&self) -> usize {
                self.0.capacity()
            }

            pub fn truncate(&mut self, len: usize) {
                self.0.truncate(len);
            }

            pub fn extend<I>(&mut self, iter: I)
            where
                I: IntoIterator<Item = $inner>,
            {
                self.0.extend(iter);
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl std::ops::Index<usize> for $name {
            type Output = $inner;

            fn index(&self, index: usize) -> &Self::Output {
                &self.0[index]
            }
        }

        impl std::ops::IndexMut<usize> for $name {
            fn index_mut(&mut self, index: usize) -> &mut Self::Output {
                &mut self.0[index]
            }
        }

        impl IntoIterator for $name {
            type Item = $inner;
            type IntoIter = std::vec::IntoIter<$inner>;

            fn into_iter(self) -> Self::IntoIter {
                self.0.into_iter()
            }
        }

        impl<'a> IntoIterator for &'a $name {
            type Item = &'a $inner;
            type IntoIter = std::slice::Iter<'a, $inner>;

            fn into_iter(self) -> Self::IntoIter {
                self.0.iter()
            }
        }

        impl<'a> IntoIterator for &'a mut $name {
            type Item = &'a mut $inner;
            type IntoIter = std::slice::IterMut<'a, $inner>;

            fn into_iter(self) -> Self::IntoIter {
                self.0.iter_mut()
            }
        }

        impl From<Vec<$inner>> for $name {
            fn from(vec: Vec<$inner>) -> Self {
                Self(vec)
            }
        }

        impl<T: Into<$inner>> FromIterator<T> for $name {
            fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
                Self(iter.into_iter().map(Into::into).collect())
            }
        }

        impl std::ops::Deref for $name {
            type Target = [$inner];

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl std::ops::DerefMut for $name {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }
    };
}
