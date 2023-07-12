use rustix::fs::{renameat_with, RenameFlags, CWD};

fn main() {
    renameat_with(CWD, "./debug/1", CWD, "./debug/2", RenameFlags::EXCHANGE).unwrap();
}
