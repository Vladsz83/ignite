/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.planner;

import java.util.List;
import org.apache.calcite.plan.RelOptPlanner.CannotPlanException;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteHashJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSort;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.calcite.rel.RelFieldCollation.Direction.ASCENDING;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/** */
public class HashJoinPlannerTest extends AbstractPlannerTest {
    /** */
    private static final String[] DISABLED_RULES = {"NestedLoopJoinConverter", "CorrelatedNestedLoopJoin", "MergeJoinConverter",
        "JoinCommuteRule"};

    /** */
    private static final String[] JOIN_TYPES = {"LEFT", "RIGHT", "INNER", "FULL OUTER"};

    /** */
    private static int tableId = 1;

    /** */
    @Test
    public void testHashJoinKeepsLeftCollation() throws Exception {
        TestTable tbl1 = createSimpleTable();
        TestTable tbl2 = createComplexTable();

        IgniteSchema schema = createSchema(tbl1, tbl2);

        String sql = "select t1.ID, t2.ID1 "
                + "from TEST_TBL_CMPLX t2 "
                + "join TEST_TBL t1 on t1.id = t2.id1 "
                + "order by t2.ID1 NULLS LAST, t2.ID2 NULLS LAST";

        RelNode plan = physicalPlan(sql, schema, DISABLED_RULES);

        assertEquals(0, findNodes(plan, byClass(IgniteSort.class)).size());
        assertEquals(1, findNodes(plan, byClass(IgniteHashJoin.class)).size());
        assertNotNull(findFirstNode(plan, byClass(IgniteHashJoin.class)));
    }

    /** */
    @Test
    public void testHashJoinErasesRightCollation() throws Exception {
        TestTable tbl1 = createSimpleTable();
        TestTable tbl2 = createComplexTable();

        IgniteSchema schema = createSchema(tbl1, tbl2);

        String sql = "select t1.ID, t2.ID1 "
                + "from TEST_TBL t1 "
                + "join TEST_TBL_CMPLX t2 on t1.id = t2.id1 "
                + "order by t2.ID1 NULLS LAST, t2.ID2 NULLS LAST";

        IgniteRel plan = physicalPlan(sql, schema, DISABLED_RULES);

        assertNotNull(findFirstNode(plan, byClass(IgniteHashJoin.class)));
        assertNotNull(sortOnTopOfJoin(plan));
    }

    /** */
    @Test
    public void testHashJoinWinsOnSkewedLeftInput() throws Exception {
        TestTable thinTblSortedPk = createSimpleTable("SMALL_TBL", 1000);
        TestTable thickTblSortedPk = createSimpleTable("LARGE_TBL", 500_000);

        IgniteSchema schema = createSchema(thinTblSortedPk, thickTblSortedPk);

        assertPlan(
            "select t1.ID, t1.ID2, t2.ID, t2.ID2 from LARGE_TBL t1 join SMALL_TBL t2 on t1.ID2 = t2.ID2",
            schema,
            nodeOrAnyChild(isInstanceOf(IgniteHashJoin.class)),
            "JoinCommuteRule"
        );

        assertPlan(
            "select t1.ID, t1.ID2, t2.ID, t2.ID2 from SMALL_TBL t1 join LARGE_TBL t2 on t1.ID2 = t2.ID2",
            schema,
            nodeOrAnyChild(isInstanceOf(IgniteHashJoin.class).negate()),
            "JoinCommuteRule"
        );

        // Merge join can consume less cpu resources.
        assertPlan(
            "select t1.ID, t1.ID2, t2.ID, t2.ID2 from SMALL_TBL t1 join LARGE_TBL t2 on t1.ID = t2.ID",
            schema,
            nodeOrAnyChild(isInstanceOf(IgniteHashJoin.class).negate()),
            "JoinCommuteRule"
        );
    }

    /** */
    private static @Nullable IgniteSort sortOnTopOfJoin(IgniteRel root) {
        List<IgniteSort> sortNodes = findNodes(root, byClass(IgniteSort.class)
            .and(node -> node.getInputs().size() == 1 && node.getInput(0) instanceof Join));

        if (sortNodes.size() > 1)
            throw new IllegalStateException("Unexpected count of sort nodes: exp<=1, act=" + sortNodes.size());

        return sortNodes.isEmpty() ? null : sortNodes.get(0);
    }

    /** */
    @Test
    public void testHashJoinApplied() throws Exception {
        for (List<Object> paramSet : joinAppliedParameters()) {
            assert paramSet != null && paramSet.size() == 2;

            String sql = (String)paramSet.get(0);
            boolean canBePlanned = (Boolean)paramSet.get(1);

            TestTable tbl = createTable("T1", IgniteDistributions.single(), "ID", Integer.class, "C1", Integer.class);

            IgniteSchema schema = createSchema(tbl);

            for (String type : JOIN_TYPES) {
                String sql0 = String.format(sql, type);

                if (canBePlanned)
                    assertPlan(sql0, schema, nodeOrAnyChild(isInstanceOf(IgniteHashJoin.class)), DISABLED_RULES);
                else {
                    assertThrows(null, () -> physicalPlan(sql0, schema, DISABLED_RULES), CannotPlanException.class,
                        "There are not enough rules");
                }
            }
        }
    }

    /** */
    private static List<List<Object>> joinAppliedParameters() {
        return F.asList(
            F.asList("select t1.c1 from t1 %s join t1 t2 using(c1)", true),
            F.asList("select t1.c1 from t1 %s join t1 t2 on t1.c1 = t2.c1", true),
            F.asList("select t1.c1 from t1 %s join t1 t2 ON t1.id is not distinct from t2.c1", false),
            F.asList("select t1.c1 from t1 %s join t1 t2 on t1.c1 = OCTET_LENGTH('TEST')", false),
            F.asList("select t1.c1 from t1 %s join t1 t2 on t1.c1 = LOG10(t1.c1)", false),
            F.asList("select t1.c1 from t1 %s join t1 t2 on t1.c1 = t2.c1 and t1.ID > t2.ID", false),
            F.asList("select t1.c1 from t1 %s join t1 t2 on t1.c1 = 1 and t2.c1 = 1", false),
            F.asList("select t1.c1 from t1 %s join t1 t2 on t1.c1 = 1", false),
            F.asList("select t1.c1 from t1 %s join t1 t2 on t1.c1 = ?", false)
        );
    }

    /** */
    private static TestTable createSimpleTable() {
        return createSimpleTable("TEST_TBL", DEFAULT_TBL_SIZE);
    }

    /** */
    private static TestTable createSimpleTable(String name, int size) {
        return createTable(
            name,
            size,
            IgniteDistributions.affinity(0, ++tableId, 0),
            "ID", Integer.class,
            "ID2", Integer.class,
            "VAL", String.class
        ).addIndex(
            RelCollations.of(new RelFieldCollation(0, ASCENDING, RelFieldCollation.NullDirection.LAST)),
            "PK"
        );
    }

    /** */
    private static TestTable createComplexTable() {
        return createTable(
            "TEST_TBL_CMPLX",
            DEFAULT_TBL_SIZE,
            IgniteDistributions.affinity(ImmutableIntList.of(0, 1), ++tableId, 0),
            "ID1", Integer.class,
            "ID2", Integer.class,
            "VAL", String.class
        ).addIndex(
            RelCollations.of(
                new RelFieldCollation(0, ASCENDING, RelFieldCollation.NullDirection.LAST),
                new RelFieldCollation(1, ASCENDING, RelFieldCollation.NullDirection.LAST)
            ),
            "PK"
        );
    }

//    /** */
//    static IgniteTable createSimpleTableHashPk(String tableName, int size) {
//        return TestBuilders.table()
//            .name(tableName)
//            .size(size)
//            .distribution(someAffinity())
//            .addColumn("ID", NativeTypes.INT32)
//            .addColumn("ID2", NativeTypes.INT32)
//            .addColumn("VAL", NativeTypes.STRING)
//            .hashIndex()
//            .name("PK")
//            .addColumn("ID")
//            .end()
//            .build();
//    }


}
