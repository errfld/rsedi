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
