package simpledb;
import java.util.*;
import javax.swing.*;
import javax.swing.tree.*;

/** The JoinOptimizer class is responsible for ordering a series of
 *    joins optimally, and for selecting the best instantiation of a
 *    join for a given logical plan.
*/
public class JoinOptimizer {
    LogicalPlan p;
    Vector<LogicalJoinNode> joins;

    /** Constructor
        @param p the logical plan being optimized
        @param joins the list of joins being performed
    */
    public JoinOptimizer(LogicalPlan p, Vector<LogicalJoinNode> joins) {
        this.p = p;
        this.joins = joins;
    }

    /** Return best iterator for computing a given logical join, given
     *   the specified statistics, and the provided left and right
     *   subplans.  Note that there is insufficient information to
     *   determine which plan should be the inner/outer here --
     *   because DbIterator's don't provide any cardinality estimates,
     *   and stats only has information about the base tables.  For
     *   this reason, the plan1
     * 
     *  @param lj The join being considered
     *  @param plan1 The left join node's child
     *  @param plan2 The right join node's child
     */
    public DbIterator instantiateJoin(LogicalJoinNode lj, DbIterator plan1, DbIterator plan2, HashMap<String, TableStats> stats) throws ParsingException {

        int t1id=0, t2id=0;
        DbIterator j;

        try {
            t1id = plan1.getTupleDesc().nameToId(p.disambiguateName(lj.f1));
        } catch (NoSuchElementException e) {
            throw new ParsingException("Unknown field " + lj.f1);
        }
        
        if (lj instanceof LogicalSubplanJoinNode)  {
            t2id = 0;
        } else {
            try {
                t2id = plan2.getTupleDesc().nameToId(p.disambiguateName(lj.f2));
            } catch (NoSuchElementException e) {
                throw new ParsingException("Unknown field " + lj.f2);
            }
        }
        
        JoinPredicate p = new JoinPredicate(t1id,lj.p,t2id);
       
        j = new Join(p,plan1,plan2);
        
        return j;

    }
    
    /**
     * Estimate the cost of a join.
     * 
     * The cost of the join should be calculated based on the join
     * algorithm (or algorithms) that you implemented for Lab 2.  It
     * should be a function of the amount of data that must be read
     * over the course of the query, as well as the number of CPU opertions
     * performed by your join.  Assume that the cost of a single predicate application
     * is roughly 1.
     *
     * 
     * @param j A LogicalJoinNode representing the join operation being performed.
     * @param card1 Estimated cardinality of the left-hand side of the query
     * @param card2 Estimated cardinality of the right-hand side of the query
     * @param cost1 Estimated cost of one full scan of the table on the left-hand side of the query
     * @param cost2 Estimated cost of one full scan of the table on the right-hand side of the query
     * @return An estimate of the cost of this query, in terms of cost1 and cost2
     */
    public double estimateJoinCost(LogicalJoinNode j, int card1, int card2, double cost1, double cost2) {
        if (j instanceof LogicalSubplanJoinNode) {
        	// A LogicalSubplanJoinNode represents a subquery.
        	// You do not need to implement proper support for these for Lab 4.
        	return card1 + cost1 + cost2;
        } else {
            // Insert your code here.
            // HINT:  You may need to use the variable "j" if you implemented a join
            //        algorithm that's more complicated than a basic nested-loops join.
            return -1.0;
        }
    }

    /**
     * Estimate the cardinality of a join.  The cardinality of a join
     * is the number of tuples produced by the join.
     * 
     * @param j A LogicalJoinNode representing the join operation
     *   being performed.
     * @param card1 Cardinality of the left-hand table in the join
     * @param card2 Cardinality of the right-hand table in the join
     * @param t1pkey Is the left-hand table a primary-key table?
     * @param t2pkey Is the right-hand table a primary-key table?
     * @return The cardinality of the join
     */
    public int estimateJoinCardinality(LogicalJoinNode j, int card1, int card2, boolean t1pkey, boolean t2pkey) {
        if (j instanceof LogicalSubplanJoinNode) {
            // A LogicalSubplanJoinNode represents a subquery.
            // You do not need to implement proper support for these for Lab 4.
            return card1;
        } else {
            // some code goes here
            return -1;
        }
    }

    /** Helper method to enumerate all of the subsets of a given size
        of a specified vector.
        @param v The vector whose subsets are desired
        @param size The size of the subsets of interest
        @return a set of all subsets of the specified size
    */
    @SuppressWarnings("unchecked")
    public <T> Set<Set<T>> enumerateSubsets(Vector<T> v, int size) {
        Set<Set<T>> els = new HashSet<Set<T>>();
        els.add(new HashSet<T>());
        Iterator<Set> it;

        long start = System.currentTimeMillis();
        for (int i = 0 ; i < size; i++) {
            Set<Set<T>> newels = new HashSet<Set<T>>();
            for (Set s : els) {
                    for (T t : v) {
                        Set<T> news = (Set)(((HashSet<T>)s).clone());
                        if (news.add(t))
                            newels.add(news);
                    }
            }
            els = newels;
        }
        
        return els;
            
    }

    /**
     * Compute a logical, reasonably efficient join on the specified
     *   tables.  See PS4 for hints on how this should be implemented.
     * 
     * @param stats Statistics for each table involved in the join,
     *    referenced by table name
     * @param filterSelectivities Selectivities of the filter
     *    predicates on each table in the join, referenced by table
     *    name
     * @param explain Indicates whether your code should explain its
     *    query plan or simply execute it
     * @return A Vector<LogicalJoinNode> that stores joins in the
     *    left-deep order in which they should be executed.
     * @throws ParsingException when stats or filter selectivities is
     *    missing a table in the join, or or when another internal
     *    error occurs
     */
    public Vector<LogicalJoinNode> orderJoins(HashMap<String, TableStats> stats, 
                                              HashMap<String, Double> filterSelectivities,  
                                              boolean explain) throws ParsingException 
    {
        //Not necessary for labs 1--3

        // some code goes here
        //Replace the following
        return joins;
    } 
 
    //===================== Private Methods =================================

    /** This is a helper method that computes the cost and cardinality
        of joining joinToRemove to joinSet (joinSet should contain
        joinToRemove), given that all of the subsets of size
        joinSet.size() - 1 have already been computed and stored in
        PlanCache pc.

        @param stats table stats for all of the base tables (see
               {@link #orderJoins})
        @param filterSelectivities the selectivities of the filters
               over each of the tables (where tables are indentified
               by their alias or name if no alias is given)
        @param joinToRemove the join to remove from joinSet
        @param joinSet the set of joins being considered
        @param bestCostSoFar the best way to join joinSet so far
               (minimum of previous invocations of
               computeCostAndCardOfSubplan for this joinSet, from
               returned CostCard)
        @param pc the PlanCache for this join; should have subplans
               for all plans of size joinSet.size()-1
        @return A {@link CostCard} objects desribing the cost,
                cardinality, optimal subplan
        @throws ParsingException when stats, filterSelectivities, or
                pc object is missing tables involved in join
    */
@SuppressWarnings("unchecked")
    private CostCard computeCostAndCardOfSubplan(HashMap<String, TableStats> stats, 
                                                HashMap<String, Double> filterSelectivities, 
                                                LogicalJoinNode joinToRemove,  
                                                Set<LogicalJoinNode> joinSet,
                                                double bestCostSoFar,
                                                PlanCache pc) throws ParsingException
    {

        LogicalJoinNode j = joinToRemove;
        
        Vector<LogicalJoinNode> prevBest;

        if (stats.get(j.t1) == null)
            throw new ParsingException("Unknown table " + j.t1);
        if (stats.get(j.t2) == null)
            throw new ParsingException("Unknown table " + j.t2);

        Set<LogicalJoinNode> news = (Set<LogicalJoinNode>) ((HashSet)joinSet).clone();
        news.remove(j);

        double t1cost,t2cost;
        int t1card,t2card;
        boolean leftPkey, rightPkey;

        if (news.isEmpty()) { //base case -- both are base relations
            prevBest = new Vector<LogicalJoinNode>();
            t1cost = stats.get(j.t1).estimateScanCost();
            t1card = stats.get(j.t1).estimateTableCardinality(filterSelectivities.get(j.t1));
            leftPkey = isPkey(j.t1, j.f1);

            t2cost = j.t2==null?0:stats.get(j.t2).estimateScanCost();
            t2card = j.t2==null?0:stats.get(j.t2).estimateTableCardinality(filterSelectivities.get(j.t2));
            rightPkey = j.t2==null?false:isPkey(j.t2,j.f2);
        } else {
            //news is not empty -- figure best way to join j to news
            prevBest = pc.getOrder(news);

            //possible that we have not cached an answer, if subset
            // includes a cross product
            if (prevBest == null) {
                return null;
            }

            double prevBestCost = pc.getCost(news);
            int bestCard = pc.getCard(news);

            t1cost = prevBestCost;  //left side just has cost of whatever left subtree is
            t1card = bestCard;
            leftPkey = hasPkey(prevBest);

            //estimate cost of right subtree
            if (doesJoin(prevBest,j.t1)) { //j.t1 is in prevBest
                t2cost = j.t2 == null?0:stats.get(j.t2).estimateScanCost();
                t2card = j.t2 == null?0:stats.get(j.t2).estimateTableCardinality(filterSelectivities.get(j.t2));
                rightPkey = j.t2 == null?false:isPkey(j.t2,j.f2);
            } else if (doesJoin(prevBest, j.t2)) { //j.t2 is in prevbest (both shouldn't be)
                t2cost = stats.get(j.t1).estimateScanCost();
                t2card = stats.get(j.t1).estimateTableCardinality(filterSelectivities.get(j.t1));
                rightPkey = isPkey(j.t1,j.f1);

            } else {
                //don't consider this plan if one of j.t1 or j.t2
                //isn't a table joined in prevBest (cross product)
                return null;
            }
        }
                    
        //case where prevbest is left
        double cost1 = estimateJoinCost(j,t1card,t2card, t1cost,t2cost);

        LogicalJoinNode j2 = j.swapInnerOuter();
        double cost2 = estimateJoinCost(j2,t2card,t1card, t2cost,t1cost);
        if (cost2 < cost1) {
            boolean tmp;
            j = j2;
            cost1 = cost2;
            tmp = rightPkey;
            rightPkey = leftPkey;
            leftPkey = tmp;
        }
        if (cost1 >= bestCostSoFar)
            return null;

        CostCard cc = new CostCard();
        cc.card = estimateJoinCardinality(j, t1card, t2card, leftPkey, rightPkey);
        cc.cost = cost1;
        cc.plan = (Vector<LogicalJoinNode>)prevBest.clone();
        cc.plan.addElement(j);  //prevbest is left -- add new join to end
        return cc;
    } 

    /** Return true if the specified table is in the list of joins, false otherwise */
    private boolean doesJoin(Vector<LogicalJoinNode> joinlist, String table) {
        for (LogicalJoinNode j : joinlist) {
            if (j.t1.equals(table) || (j.t2!=null && j.t2.equals(table)))
                return true;
        }
        return false;
    }

    /** Return true if field is a primary key of the specified table, false otherwise */
    private boolean isPkey(String table, String field) {
        int tid1 = p.getTableId(table);
        String pkey1 = Database.getCatalog().getPrimaryKey(tid1);

        return (pkey1.equals(field) || (table + "." + pkey1).equals(field));
    }

    /** Return true if a primary key field is joined by one of the joins in joinlist */
    private boolean hasPkey(Vector<LogicalJoinNode> joinlist) {
        for (LogicalJoinNode j: joinlist) {
            if (isPkey(j.t1, j.f1) || (j.t2!=null && isPkey(j.t2, j.f2))) return true;
        }
        return false;
        
    }
    
    /** Helper function to display a Swing window with a tree representation of the
        specified list of joins.  See {@link #orderJoins}, which may want to call this
        when the analyze flag is true.

        @param js the join plan to visualize
        @param pc the PlanCache accumulated whild building the optimal
               plan
        @param stats table statistics for base tables
        @param selectivities the selectivities of the filters over each
                of the tables (where tables are indentified by their
                alias or name if no alias is given)
    */
    private void printJoins(Vector<LogicalJoinNode> js, 
                           PlanCache pc,
                           HashMap<String, TableStats> stats,
                           HashMap<String,Double> selectivities ) {

        JFrame f = new JFrame("Join Plan for " + p.getQuery());
 
        // Set the default close operation for the window, 
        // or else the program won't exit when clicking close button
        f.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
 
        f.setVisible(true);

        f.setSize(300,500);

        HashMap<String, DefaultMutableTreeNode> m = new HashMap<String, DefaultMutableTreeNode>();

        int numTabs = 0;
        
        int k;
        DefaultMutableTreeNode root = null, treetop = null;
        HashSet<LogicalJoinNode> pathSoFar = new HashSet<LogicalJoinNode>();
        boolean neither;

        System.out.println(js);
        for (LogicalJoinNode j : js) {
            pathSoFar.add(j);
            System.out.println("PATH SO FAR = " + pathSoFar);

            Double c = pc.getCost(pathSoFar);
            neither = true;

            root = new DefaultMutableTreeNode("Join " + j + " (Cost =" + pc.getCost(pathSoFar) + 
                                              ", card = " + pc.getCard(pathSoFar)+ ")");
            DefaultMutableTreeNode n = m.get(j.t1);
            if (n == null) {  //never seen this table before
                n  = new DefaultMutableTreeNode(j.t1 + " (Cost = " + stats.get(j.t1).estimateScanCost()+ ", card = " +
                                                stats.get(j.t1).estimateTableCardinality(selectivities.get(j.t1))+ ")");
                root.add(n);
            }  else {
                //make left child root n
                root.add(n);
                neither = false;
            }
            m.put(j.t1, root);

            n = m.get(j.t2);
            if (n == null) { //never seen this table before
                
                n  = new DefaultMutableTreeNode(j.t2==null?"Subplan":(j.t2 + " (Cost = " + 
                                                                      stats.get(j.t2).estimateScanCost()+ ", card = " +
                                                                      stats.get(j.t2).estimateTableCardinality(selectivities.get(j.t1))+ 
                                                                      ")"));
                root.add(n);
            }  else {
                //make right child root n
                root.add(n);
                neither = false;
            }
            m.put(j.t2, root);
            
            //unless this table doesn't join with other tables,
            // all tables are accessed from root
            if (!neither) {
                for (String key : m.keySet()) {
                    m.put(key, root);
                }
            }

            treetop = root;
        }

        JTree tree = new JTree(treetop);
        JScrollPane treeView = new JScrollPane(tree);

        tree.setShowsRootHandles(true);

        //Set the icon for leaf nodes.
        ImageIcon leafIcon = new ImageIcon("join.jpg");
        if (leafIcon != null) {
            DefaultTreeCellRenderer renderer = new DefaultTreeCellRenderer();
            renderer.setOpenIcon(leafIcon);
            renderer.setClosedIcon(leafIcon);

            tree.setCellRenderer(renderer);
        } else {
            System.err.println("Leaf icon missing; using default.");
        }

        f.setSize(300,500);

        f.add(treeView);
        for (int i = 0; i < tree.getRowCount(); i++) {
            tree.expandRow(i);
        }

        if (js.size() == 0) {
            f.add(new JLabel("No joins in plan."));
        }

        f.pack();

    }   

}