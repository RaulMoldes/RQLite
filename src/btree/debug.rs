use super::*;
use std::fmt;

impl<K, Vl, Vi, P> fmt::Display for BTree<K, Vl, Vi, P>
where
    K: Ord + Clone + Copy + Eq + PartialEq + fmt::Debug,
    Vl: Cell<Key = K> + fmt::Debug,
    Vi: Cell<Key = K> + fmt::Debug,
    P: Send
        + Sync
        + HeaderOps
        + InteriorPageOps<Vi>
        + LeafPageOps<Vl>
        + Overflowable<LeafContent = Vl>
        + fmt::Debug,
    PageFrame<P>: TryFrom<IOFrame, Error = std::io::Error>,
    IOFrame: TryFrom<PageFrame<P>, Error = std::io::Error>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "BTree {{")?;
        writeln!(f, "  Type: {:?}", self.tree_type)?;
        writeln!(f, "  Root: {}", u32::from(self.root))?;
        writeln!(f, "  Max Payload Fraction: {}", self.max_payload_fraction)?;
        writeln!(f, "  Min Payload Fraction: {}", self.min_payload_fraction)?;
        writeln!(f, "}}")?;
        Ok(())
    }
}

impl<K, Vl, Vi, P> BTree<K, Vl, Vi, P>
where
    K: Ord + Clone + Copy + Eq + PartialEq + fmt::Debug,
    Vl: Cell<Key = K> + fmt::Debug,
    Vi: Cell<Key = K> + fmt::Debug,
    P: Send
        + Sync
        + HeaderOps
        + InteriorPageOps<Vi>
        + LeafPageOps<Vl>
        + Overflowable<LeafContent = Vl>
        + fmt::Debug,
    PageFrame<P>: TryFrom<IOFrame, Error = std::io::Error>,
    IOFrame: From<PageFrame<P>>,
{
    /// Print the tree structure with detailed information about each node
    pub(crate) fn print_tree<FI: FileOps, M: MemoryPool>(
        &self,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {
        println!("\n{:=<60}", ""); // top separator
        println!("BTREE STRUCTURE");
        println!("{:=<60}", ""); // underline
        println!("Type: {:?}", self.tree_type);
        println!("Root Page ID: {}", u32::from(self.root));
        println!("Max Payload Fraction: {}", self.max_payload_fraction);
        println!("Min Payload Fraction: {}", self.min_payload_fraction);
        println!("{:=<60}\n", ""); // another separator

        self.print_node(self.root, 0, pager, "ROOT")?;

        println!("\n{:=<60}", ""); // bottom separator
        Ok(())
    }

    /// Recursively print a node and its children
    fn print_node<FI: FileOps, M: MemoryPool>(
        &self,
        page_id: PageId,
        depth: usize,
        pager: &mut Pager<FI, M>,
        label: &str,
    ) -> std::io::Result<()> {
        let indent = "  ".repeat(depth);
        let io_frame = pager.get_single(page_id)?;

        println!(
            "{}┌─ {} [ID: {}] {:?}",
            indent,
            label,
            u32::from(page_id),
            io_frame.page_type()
        );

        if io_frame.is_leaf() {
            self.print_leaf_node(io_frame, depth, pager)?;
        } else if io_frame.is_interior() {
            self.print_interior_node(io_frame, depth, pager)?;
        }

        Ok(())
    }

    /// Print a leaf node's contents
    fn print_leaf_node<FI: FileOps, M: MemoryPool>(
        &self,
        io_frame: IOFrame,
        depth: usize,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {
        let indent = "  ".repeat(depth + 1);
        let frame = PageFrame::try_from(io_frame)?;
        let guard = frame.read();

        let mut cells: Vec<Vl> = Vec::with_capacity(guard.cell_count() as usize);
        for i in 0..guard.cell_count() {
            cells.push(guard.get_cell_at_leaf(i).unwrap());
        }
        let cell_count = cells.len();
        let total_size: u16 = cells.iter().map(|c| c.size()).sum();

        println!(
            "{}│  Cells: {} | Total Size: {} bytes",
            indent, cell_count, total_size
        );

        for (idx, cell) in cells.iter().enumerate() {
            let has_overflow = cell.overflow_page().is_some();
            let overflow_marker = if has_overflow { " [OVERFLOW]" } else { "" };

            println!(
                "{}│  [{}] Key: {:?} | Size: {} bytes{}",
                indent,
                idx,
                cell.key(),
                cell.size(),
                overflow_marker
            );

            // Si hay overflow, mostrar la cadena
            if let Some(overflow_id) = cell.overflow_page() {
                self.print_overflow_chain(overflow_id, depth + 2, pager)?;
            }
        }

        println!("{}└─", indent);
        Ok(())
    }

    /// Print an interior node and recursively print its children
    fn print_interior_node<FI: FileOps, M: MemoryPool>(
        &self,
        io_frame: IOFrame,
        depth: usize,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {
        let indent = "  ".repeat(depth + 1);
        let frame = PageFrame::try_from(io_frame)?;
        let guard = frame.read();

        let mut cells: Vec<Vi> = Vec::with_capacity(guard.cell_count() as usize);
        for i in 0..guard.cell_count() {
            cells.push(guard.get_cell_at_interior(i).unwrap());
        }
        let rightmost = guard.get_rightmost_child();

        println!("{}│  Interior Cells: {}", indent, cells.len());

        // Print each child recursively
        for (idx, cell) in cells.iter().enumerate() {
            if let Some(child_id) = cell.left_child() {
                println!("{}│", indent);
                self.print_node(
                    child_id,
                    depth + 1,
                    pager,
                    &format!("Child[{}] Key: {:?}", idx, cell.key()),
                )?;
            }
        }

        // Print rightmost child if it exists
        if let Some(rightmost_id) = rightmost {
            println!("{}│", indent);
            self.print_node(rightmost_id, depth + 1, pager, "RIGHTMOST")?;
        }

        println!("{}└─", indent);
        Ok(())
    }

    /// Print the overflow chain for a cell
    fn print_overflow_chain<FI: FileOps, M: MemoryPool>(
        &self,
        first_overflow_id: PageId,
        depth: usize,
        pager: &mut Pager<FI, M>,
    ) -> std::io::Result<()> {
        let indent = "  ".repeat(depth);
        let mut current_id = Some(first_overflow_id);
        let mut chain_index = 0;

        while let Some(overflow_id) = current_id {
            let io_frame = pager.get_single(overflow_id)?;
            let overflow_frame = PageFrame::<OverflowPage>::try_from(io_frame)?;
            let guard = overflow_frame.read();

            let data_size = guard.data.as_bytes().len();
            let has_next = guard.get_next_overflow().is_some();
            let next_marker = if has_next { " → " } else { " (END)" };

            println!(
                "{}    ↳ Overflow[{}] ID: {} | Size: {} bytes{}",
                indent,
                chain_index,
                u32::from(overflow_id),
                data_size,
                next_marker
            );

            current_id = guard.get_next_overflow();
            chain_index += 1;
        }

        Ok(())
    }
}
