use std::{any::Any, io};

trait Operator<T> {
    fn open(&mut self, inputs: &[Box<dyn Operator<T>>]) -> io::Result<()>;
    fn try_next(&mut self) -> Option<T>;
    fn close(&mut self);
}

enum OperatorType {
    Logical,
    Physical,
}

trait Rule<T: Any> {
    fn apply(&self, inputs: Vec<Box<dyn Operator<T>>>) -> io::Result<Vec<Box<dyn Operator<T>>>>;
}
