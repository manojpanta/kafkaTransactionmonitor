package com.kafka.transactionmonitor;


import com.sun.source.tree.Tree;

public class BinarySearchTree {
    public static void main(String[] args) {
        TreeNode root = new TreeNode(10);
        root.left = new TreeNode(5);
        root.right = new TreeNode(15);
        root.left.left = new TreeNode(3);
        root.left.right = new TreeNode(7);
        root.right.left = new TreeNode(12);
        root.right.right = new TreeNode(18);
        System.out.println(" ----------inserting records in tree ----------");
        int number = 2;
        TreeNode node = insertToBinarySearchTree(root, number, 0);
        System.out.println("Inserted to Node: " + node);
        System.out.println(" ----------Searching records in tree ----------");
        int target = 3;
        boolean found = searchBinarySearchTree(root, target, 0);
        System.out.println("target is found: " + found);
    }

    private static TreeNode insertToBinarySearchTree(TreeNode root, int number, int height) {
        if (root == null) {
            height ++;
            System.out.println("Height at insertion: " + height);
            root = new TreeNode(number);
            return root;
        }
        if (root.value > number){
            return insertToBinarySearchTree(root.left, number, height + 1);
        } else if (root.value < number) {
            return insertToBinarySearchTree(root.right, number, height + 1);
        }
        return root;
    }

    private static boolean searchBinarySearchTree(TreeNode root, int target, int height) {
        if (root == null) {
            return false;
        } else {
            height += 1;
        }
        if (root.value == target) {
            System.out.println("Found in node: " + root);
            System.out.println("Found at height: " + height);
            return true;
        } else if (root.value > target){;// we use recursive function to find the element.
            return searchBinarySearchTree(root.left, target, height);
        } else {
            return searchBinarySearchTree(root.right, target, height);
        }
    }


}
class TreeNode {
    int value;
    TreeNode left;
    TreeNode right;

    TreeNode(int value) {
        this.value = value;
        left = null;
        right = null;
    }

    @Override
    public String toString() {
        return "TreeNode{" +
                "value=" + value +
                ", left=" + left +
                ", right=" + right +
                '}';
    }
}


/*
         10
          |
     ___________
     |         |
     5        15
     |         |
   ______   ______
   |    |   |    |
   3    7  12    18
 */