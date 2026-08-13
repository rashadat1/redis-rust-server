use std::collections::HashMap;
#[derive(Clone)]
pub struct StreamNode {
    pub prefix: String,
    pub data: HashMap<String, String>,
    pub children: HashMap<String, StreamNode>,
}
impl StreamNode {
    pub fn new(prefix: String, kv_pairs: Option<HashMap<String, String>>) -> Self {
        let data = match kv_pairs {
            None => HashMap::new(),
            Some(kv) => kv,
        };
        let children: HashMap<String, StreamNode> = HashMap::new();
        return StreamNode {
            prefix,
            data,
            children,
        };
    }
}
pub struct Stream {
    pub root: StreamNode,
    pub last_id: String,
}
impl Stream {
    pub fn new() -> Self {
        let root = StreamNode::new("".to_string(), None);
        return Stream {
            root,
            last_id: String::from("0-0"),
        };
    }

    pub fn insert(&mut self, insert_idx: String, insert_data: Option<HashMap<String, String>>) {
        let root = &mut self.root;
        let mut total_consumed = 0;
        // first check if any of the root's children have a common character with the insert
        // idx leading char
        let mut curr = root;
        loop {
            let insert_frag = insert_idx.get(total_consumed..).unwrap().to_string();
            let child_shared_prefix = child_with_shared_prefix(&curr.children, insert_frag.clone());
            if child_shared_prefix.is_none() {
                // no child has common characters with the insert frag so we insert here as a new
                // child
                curr.children.insert(
                    insert_frag.clone(),
                    StreamNode::new(insert_frag.clone(), insert_data.clone()),
                );
                self.last_id = insert_idx;
                return;
            }
            let child_node_prefix = child_shared_prefix.unwrap();
            let (intermediate_node_prefix, num_consumed) =
                find_shared_prefix(child_node_prefix.clone(), insert_frag.clone());

            total_consumed += num_consumed;
            if num_consumed >= child_node_prefix.len() {
                if total_consumed >= insert_idx.len() {
                    // if we consumed the entire node to insert then we dont try to split - we just
                    // update the current node's data
                    let node = curr.children.get_mut(&child_node_prefix).unwrap();
                    node.data = insert_data.unwrap_or_default();
                    self.last_id = insert_idx;
                    return;
                }
                // if we consumed the entire child node prefix then we trace to the next part of
                // the tree - the children of the child
                curr = curr.children.get_mut(&child_node_prefix).unwrap();
                continue;
            }
            // if we did not consume the entire child node prefix then that means we have some
            // non-shared suffix so we split here and insert the new node
            let mut intermediate_node = StreamNode::new(intermediate_node_prefix, None);
            let insert_node = StreamNode::new(
                insert_idx.get(total_consumed..).unwrap().to_string(),
                insert_data.clone(),
            );
            let old_child_node = curr.children.get(&child_node_prefix).unwrap().clone();
            let mut not_shared_suffix_node = StreamNode::new(
                child_node_prefix.get(num_consumed..).unwrap().to_string(),
                Some(old_child_node.data.clone()),
            );
            not_shared_suffix_node.children = old_child_node.children.clone();
            // do the placing of the new node
            curr.children.remove(&child_node_prefix);
            intermediate_node.children.insert(
                not_shared_suffix_node.prefix.clone(),
                not_shared_suffix_node,
            );
            intermediate_node
                .children
                .insert(insert_node.prefix.clone(), insert_node);

            curr.children
                .insert(intermediate_node.prefix.clone(), intermediate_node);
            self.last_id = insert_idx;
            break;
        }
    }
}
fn child_with_shared_prefix(
    node_children: &HashMap<String, StreamNode>,
    insert_node_frag: String,
) -> Option<String> {
    for (prefix, _) in node_children {
        if insert_node_frag.chars().nth(0).unwrap() == prefix.chars().nth(0).unwrap() {
            return Some(prefix.to_string());
        }
    }
    None
}
fn find_shared_prefix(curr_in_tree_node: String, insert_node_frag: String) -> (String, usize) {
    // return the shared prefix and the number of indices consumed in the node we are inserting
    let mut i = 0;
    loop {
        if i >= insert_node_frag.len() {
            break;
        }
        if i >= curr_in_tree_node.len() {
            break;
        }
        let ith_curr_tree = curr_in_tree_node.chars().nth(i).unwrap();
        // curr_in_tree_node.as_bytes().get(i).unwrap();
        let ith_insert_frag = insert_node_frag.chars().nth(i).unwrap();
        if ith_curr_tree == ith_insert_frag {
            i += 1;
        } else {
            break;
        }
    }
    (curr_in_tree_node.get(..i).unwrap().to_string(), i)
}
fn print_tree(node: &StreamNode, indent: String) {
    println!("{} └── {}", indent, node.prefix);
    for (_, child) in &node.children {
        print_tree(&child, format!("{}     ", indent));
    }
}
