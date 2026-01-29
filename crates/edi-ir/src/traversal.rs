//! Traversal and cursor APIs for navigating the IR tree

use crate::node::Node;
use crate::Error;
use crate::Result;

/// A cursor for navigating the IR tree
pub struct Cursor<'a> {
    /// Current node
    node: &'a Node,

    /// Path to current node (for error reporting)
    path: Vec<String>,
}

/// Trait for traversing the IR tree
pub trait Traversal {
    /// Visit a node
    fn visit(&mut self, node: &Node, path: &[String]);

    /// Called when entering a node with children
    fn enter(&mut self, _node: &Node, _path: &[String]) {}

    /// Called when leaving a node with children
    fn leave(&mut self, _node: &Node, _path: &[String]) {}

    /// Returns true if traversal should continue
    fn should_continue(&self) -> bool {
        true
    }
}

impl<'a> Cursor<'a> {
    /// Create a new cursor at the given node
    pub fn new(node: &'a Node) -> Self {
        Self {
            node,
            path: vec![node.name.clone()],
        }
    }

    /// Get the current node
    pub fn node(&self) -> &Node {
        self.node
    }

    /// Get the current path
    pub fn path(&self) -> &[String] {
        &self.path
    }

    /// Navigate to a child node by name
    pub fn child(&self, name: &str) -> Result<Cursor<'a>> {
        match self.node.find_child(name) {
            Some(child) => {
                let mut new_path = self.path.clone();
                new_path.push(name.to_string());
                Ok(Cursor {
                    node: child,
                    path: new_path,
                })
            }
            None => Err(Error::NodeNotFound(format!(
                "{}/{}",
                self.path.join("/"),
                name
            ))),
        }
    }

    /// Navigate to a child by index
    pub fn child_at(&self, index: usize) -> Result<Cursor<'a>> {
        match self.node.children.get(index) {
            Some(child) => {
                let mut new_path = self.path.clone();
                new_path.push(format!("[{}]", index));
                Ok(Cursor {
                    node: child,
                    path: new_path,
                })
            }
            None => Err(Error::NodeNotFound(format!(
                "{}[{}]",
                self.path.join("/"),
                index
            ))),
        }
    }

    /// Get all children matching a name
    pub fn children(&self, name: &str) -> Vec<Cursor<'a>> {
        self.node
            .find_children(name)
            .into_iter()
            .enumerate()
            .map(|(idx, child)| {
                let mut new_path = self.path.clone();
                new_path.push(format!("{}[{}]", name, idx));
                Cursor {
                    node: child,
                    path: new_path,
                }
            })
            .collect()
    }

    /// Navigate using a path (e.g., "ORDERS/MSG/ITEM[0]/LIN")
    pub fn navigate(&self, path: &str) -> Result<Cursor<'a>> {
        let mut current_node = self.node;
        let mut current_path = self.path.clone();

        for segment in path.split('/') {
            if segment.is_empty() {
                continue;
            }

            // Handle array indexing like "ITEM[0]"
            if let Some(open_bracket) = segment.find('[') {
                let name = &segment[..open_bracket];
                let close_bracket = segment.find(']').ok_or_else(|| {
                    Error::InvalidPath(format!("Unclosed bracket in: {}", segment))
                })?;
                let index: usize = segment[open_bracket + 1..close_bracket]
                    .parse()
                    .map_err(|_| Error::InvalidPath(format!("Invalid index in: {}", segment)))?;

                // Find children by name
                let children: Vec<&Node> = current_node
                    .children
                    .iter()
                    .filter(|c| c.name == name)
                    .collect();

                current_node = children.get(index).ok_or_else(|| {
                    Error::NodeNotFound(format!("{}/{}", current_path.join("/"), segment))
                })?;
                current_path.push(format!("{}[{}]", name, index));
            } else {
                current_node = current_node.find_child(segment).ok_or_else(|| {
                    Error::NodeNotFound(format!("{}/{}", current_path.join("/"), segment))
                })?;
                current_path.push(segment.to_string());
            }
        }

        // Return a new cursor with the accumulated path
        Ok(Cursor {
            node: current_node,
            path: current_path,
        })
    }
}

/// Walk the tree using a visitor
pub fn walk<T: Traversal>(node: &Node, visitor: &mut T) {
    walk_recursive(node, visitor, &mut vec![]);
}

fn walk_recursive<T: Traversal>(node: &Node, visitor: &mut T, path: &mut Vec<String>) {
    if !visitor.should_continue() {
        return;
    }

    visitor.visit(node, path);

    if !node.children.is_empty() {
        visitor.enter(node, path);
        path.push(node.name.clone());

        for child in &node.children {
            walk_recursive(child, visitor, path);
        }

        path.pop();
        visitor.leave(node, path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{Node, NodeType};

    #[test]
    fn test_cursor_creation() {
        let root = Node::new("ROOT", NodeType::Root);
        let cursor = Cursor::new(&root);

        assert_eq!(cursor.node().name, "ROOT");
        assert_eq!(cursor.path(), &["ROOT"]);
    }

    #[test]
    fn test_cursor_child() {
        let mut root = Node::new("ROOT", NodeType::Root);
        let child = Node::new("CHILD", NodeType::Segment);
        root.add_child(child);

        let cursor = Cursor::new(&root);
        let child_cursor = cursor.child("CHILD").unwrap();

        assert_eq!(child_cursor.node().name, "CHILD");
        assert_eq!(child_cursor.path(), &["ROOT", "CHILD"]);
    }

    #[test]
    fn test_cursor_child_not_found() {
        let root = Node::new("ROOT", NodeType::Root);
        let cursor = Cursor::new(&root);

        let result = cursor.child("NONEXISTENT");
        assert!(result.is_err());

        match result {
            Err(Error::NodeNotFound(path)) => {
                assert!(path.contains("ROOT"));
                assert!(path.contains("NONEXISTENT"));
            }
            _ => panic!("Expected NodeNotFound error"),
        }
    }

    #[test]
    fn test_cursor_child_at() {
        let mut root = Node::new("ROOT", NodeType::Root);
        let child1 = Node::new("CHILD1", NodeType::Segment);
        let child2 = Node::new("CHILD2", NodeType::Segment);
        root.add_child(child1);
        root.add_child(child2);

        let cursor = Cursor::new(&root);
        let child0_cursor = cursor.child_at(0).unwrap();
        let child1_cursor = cursor.child_at(1).unwrap();

        assert_eq!(child0_cursor.node().name, "CHILD1");
        assert_eq!(child1_cursor.node().name, "CHILD2");
    }

    #[test]
    fn test_cursor_child_at_not_found() {
        let root = Node::new("ROOT", NodeType::Root);
        let cursor = Cursor::new(&root);

        let result = cursor.child_at(0);
        assert!(result.is_err());

        match result {
            Err(Error::NodeNotFound(path)) => {
                assert!(path.contains("[0]"));
            }
            _ => panic!("Expected NodeNotFound error"),
        }
    }

    #[test]
    fn test_cursor_children() {
        let mut root = Node::new("ROOT", NodeType::Root);
        let item1 = Node::new("ITEM", NodeType::Segment);
        let item2 = Node::new("ITEM", NodeType::Segment);
        let other = Node::new("OTHER", NodeType::Segment);
        root.add_child(item1);
        root.add_child(other);
        root.add_child(item2);

        let cursor = Cursor::new(&root);
        let items = cursor.children("ITEM");

        assert_eq!(items.len(), 2);
        assert_eq!(items[0].node().name, "ITEM");
        assert_eq!(items[1].node().name, "ITEM");
        assert_eq!(items[0].path(), &["ROOT", "ITEM[0]"]);
        assert_eq!(items[1].path(), &["ROOT", "ITEM[1]"]);
    }

    #[test]
    fn test_cursor_children_empty() {
        let root = Node::new("ROOT", NodeType::Root);
        let cursor = Cursor::new(&root);

        let items = cursor.children("ITEM");
        assert!(items.is_empty());
    }

    #[test]
    fn test_cursor_navigate() {
        let mut root = Node::new("ROOT", NodeType::Root);
        let mut level1 = Node::new("LEVEL1", NodeType::SegmentGroup);
        let leaf = Node::new("LEAF", NodeType::Element);
        level1.add_child(leaf);
        root.add_child(level1);

        let cursor = Cursor::new(&root);
        let leaf_cursor = cursor.navigate("LEVEL1/LEAF").unwrap();

        assert_eq!(leaf_cursor.node().name, "LEAF");
        assert_eq!(leaf_cursor.path(), &["ROOT", "LEVEL1", "LEAF"]);
    }

    #[test]
    fn test_cursor_navigate_with_index() {
        let mut root = Node::new("ROOT", NodeType::Root);
        let item1 = Node::new("ITEM", NodeType::Segment);
        let item2 = Node::new("ITEM", NodeType::Segment);
        root.add_child(item1);
        root.add_child(item2);

        let cursor = Cursor::new(&root);
        let item0_cursor = cursor.navigate("ITEM[0]").unwrap();
        let item1_cursor = cursor.navigate("ITEM[1]").unwrap();

        assert_eq!(item0_cursor.node().name, "ITEM");
        assert_eq!(item1_cursor.node().name, "ITEM");
    }

    #[test]
    fn test_cursor_navigate_error() {
        let root = Node::new("ROOT", NodeType::Root);
        let cursor = Cursor::new(&root);

        // Non-existent path
        let result = cursor.navigate("NONEXISTENT");
        assert!(result.is_err());
        match result {
            Err(Error::NodeNotFound(_)) => (),
            _ => panic!("Expected NodeNotFound error"),
        }
    }

    #[test]
    fn test_cursor_navigate_invalid_index() {
        let mut root = Node::new("ROOT", NodeType::Root);
        let item = Node::new("ITEM", NodeType::Segment);
        root.add_child(item);

        let cursor = Cursor::new(&root);

        // Index out of bounds
        let result = cursor.navigate("ITEM[5]");
        assert!(result.is_err());
        match result {
            Err(Error::NodeNotFound(_)) => (),
            _ => panic!("Expected NodeNotFound error"),
        }
    }

    #[test]
    fn test_cursor_navigate_invalid_path() {
        let root = Node::new("ROOT", NodeType::Root);
        let cursor = Cursor::new(&root);

        // Unclosed bracket
        let result = cursor.navigate("ITEM[0");
        assert!(result.is_err());
        match result {
            Err(Error::InvalidPath(_)) => (),
            _ => panic!("Expected InvalidPath error"),
        }

        // Invalid index
        let result = cursor.navigate("ITEM[abc]");
        assert!(result.is_err());
        match result {
            Err(Error::InvalidPath(_)) => (),
            _ => panic!("Expected InvalidPath error"),
        }
    }

    #[test]
    fn test_cursor_navigate_empty_segments() {
        let mut root = Node::new("ROOT", NodeType::Root);
        let child = Node::new("CHILD", NodeType::Segment);
        root.add_child(child);

        let cursor = Cursor::new(&root);

        // Empty segments should be skipped
        let child_cursor = cursor.navigate("//CHILD//").unwrap();
        assert_eq!(child_cursor.node().name, "CHILD");
    }

    // Test visitor for traversal tests
    struct TestVisitor {
        visited: Vec<String>,
        entered: Vec<String>,
        left: Vec<String>,
        max_visits: usize,
    }

    impl TestVisitor {
        fn new() -> Self {
            Self {
                visited: Vec::new(),
                entered: Vec::new(),
                left: Vec::new(),
                max_visits: usize::MAX,
            }
        }

        fn with_max_visits(max: usize) -> Self {
            Self {
                visited: Vec::new(),
                entered: Vec::new(),
                left: Vec::new(),
                max_visits: max,
            }
        }
    }

    impl Traversal for TestVisitor {
        fn visit(&mut self, node: &Node, _path: &[String]) {
            self.visited.push(node.name.clone());
        }

        fn enter(&mut self, node: &Node, _path: &[String]) {
            self.entered.push(node.name.clone());
        }

        fn leave(&mut self, node: &Node, _path: &[String]) {
            self.left.push(node.name.clone());
        }

        fn should_continue(&self) -> bool {
            self.visited.len() < self.max_visits
        }
    }

    #[test]
    fn test_traversal_walk() {
        let mut root = Node::new("ROOT", NodeType::Root);
        let mut parent = Node::new("PARENT", NodeType::SegmentGroup);
        let child1 = Node::new("CHILD1", NodeType::Segment);
        let child2 = Node::new("CHILD2", NodeType::Segment);

        parent.add_child(child1);
        parent.add_child(child2);
        root.add_child(parent);

        let mut visitor = TestVisitor::new();
        walk(&root, &mut visitor);

        // Should visit ROOT, PARENT, CHILD1, CHILD2
        assert_eq!(visitor.visited, vec!["ROOT", "PARENT", "CHILD1", "CHILD2"]);
    }

    #[test]
    fn test_traversal_enter_leave() {
        let mut root = Node::new("ROOT", NodeType::Root);
        let mut parent = Node::new("PARENT", NodeType::SegmentGroup);
        let child = Node::new("CHILD", NodeType::Segment);

        parent.add_child(child);
        root.add_child(parent);

        let mut visitor = TestVisitor::new();
        walk(&root, &mut visitor);

        // Nodes with children should trigger enter/leave
        assert_eq!(visitor.entered, vec!["ROOT", "PARENT"]);
        assert_eq!(visitor.left, vec!["PARENT", "ROOT"]);

        // Leaf node (CHILD) has no children, so no enter/leave
        assert!(!visitor.entered.contains(&"CHILD".to_string()));
        assert!(!visitor.left.contains(&"CHILD".to_string()));
    }

    #[test]
    fn test_traversal_should_continue() {
        let mut root = Node::new("ROOT", NodeType::Root);
        let mut parent = Node::new("PARENT", NodeType::SegmentGroup);
        let child1 = Node::new("CHILD1", NodeType::Segment);
        let child2 = Node::new("CHILD2", NodeType::Segment);

        parent.add_child(child1);
        parent.add_child(child2);
        root.add_child(parent);

        // Limit to 2 visits
        let mut visitor = TestVisitor::with_max_visits(2);
        walk(&root, &mut visitor);

        // Should stop after visiting ROOT and PARENT
        assert_eq!(visitor.visited.len(), 2);
        assert_eq!(visitor.visited, vec!["ROOT", "PARENT"]);
    }

    #[test]
    fn test_traversal_leaf_node() {
        let leaf = Node::new("LEAF", NodeType::Element);

        let mut visitor = TestVisitor::new();
        walk(&leaf, &mut visitor);

        // Leaf node should be visited but not trigger enter/leave
        assert_eq!(visitor.visited, vec!["LEAF"]);
        assert!(visitor.entered.is_empty());
        assert!(visitor.left.is_empty());
    }

    #[test]
    fn test_traversal_deep_nesting() {
        let mut root = Node::new("ROOT", NodeType::Root);
        let mut level1 = Node::new("LEVEL1", NodeType::SegmentGroup);
        let mut level2 = Node::new("LEVEL2", NodeType::SegmentGroup);
        let mut level3 = Node::new("LEVEL3", NodeType::SegmentGroup);
        let leaf = Node::new("LEAF", NodeType::Element);

        level3.add_child(leaf);
        level2.add_child(level3);
        level1.add_child(level2);
        root.add_child(level1);

        let mut visitor = TestVisitor::new();
        walk(&root, &mut visitor);

        assert_eq!(
            visitor.visited,
            vec!["ROOT", "LEVEL1", "LEVEL2", "LEVEL3", "LEAF"]
        );
        assert_eq!(visitor.entered, vec!["ROOT", "LEVEL1", "LEVEL2", "LEVEL3"]);
        assert_eq!(visitor.left, vec!["LEVEL3", "LEVEL2", "LEVEL1", "ROOT"]);
    }
}
