/*************************************************************************
 AURA OPTIMIZED BULK LOADER — FINAL MODEL
 All CSVs must be uploaded into Neo4j Aura "Import" section
 Paths expected: file:///Import/<filename>.csv
*************************************************************************/


// ======================
// 1. Constraints (Idempotent)
// ======================
CREATE CONSTRAINT employee_pk IF NOT EXISTS
FOR (e:Employee) REQUIRE e.EmployeeID IS UNIQUE;

CREATE CONSTRAINT job_pk IF NOT EXISTS
FOR (j:Job) REQUIRE j.JobID IS UNIQUE;

CREATE CONSTRAINT location_pk IF NOT EXISTS
FOR (l:Location) REQUIRE l.LocationID IS UNIQUE;

CREATE CONSTRAINT skill_pk IF NOT EXISTS
FOR (s:Skill) REQUIRE s.SkillID IS UNIQUE;


// ======================
// 2. Load Employee Nodes
// ======================
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Import/employees.csv' AS row
MERGE (e:Employee {EmployeeID: row.EmployeeID})
SET e += apoc.map.clean(row, ["EmployeeID"], [null]);


// ======================
// 3. Load Job Nodes
// ======================
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Import/jobs.csv' AS row
MERGE (j:Job {JobID: row.JobID})
SET j += apoc.map.clean(row, ["JobID"], [null]);


// ======================
// 4. Load Location Nodes
// ======================
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Import/locations.csv' AS row
MERGE (l:Location {LocationID: row.LocationID})
SET l += apoc.map.clean(row, ["LocationID"], [null]);


// ======================
// 5. Load Skill Nodes
// ======================
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Import/skills.csv' AS row
MERGE (s:Skill {SkillID: row.SkillID})
SET s += apoc.map.clean(row, ["SkillID"], [null]);


// ===================================================
// 6. Relationship: (:Employee)-[:WORKS_AS]->(:Job)
// ===================================================
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Import/employee_jobs.csv' AS row
MATCH (e:Employee {EmployeeID: row.EmployeeID})
MATCH (j:Job {JobID: row.JobID})
MERGE (e)-[:WORKS_AS]->(j);


// =======================================================
// 7. Relationship: (:Employee)-[:LOCATED_IN]->(:Location)
// =======================================================
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Import/employee_locations.csv' AS row
MATCH (e:Employee {EmployeeID: row.EmployeeID})
MATCH (l:Location {LocationID: row.LocationID})
MERGE (e)-[:LOCATED_IN]->(l);


// ====================================================
// 8. Relationship: (:Employee)-[:HAS_SKILL]->(:Skill)
// ====================================================
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Import/employee_skills.csv' AS row
MATCH (e:Employee {EmployeeID: row.EmployeeID})
MATCH (s:Skill {SkillID: row.SkillID})
MERGE (e)-[:HAS_SKILL]->(s);


// ==================================================
// 9. Relationship: (:Job)-[:REQUIRES_SKILL]->(:Skill)
// ==================================================
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Import/requires_skill.csv' AS row
MATCH (j:Job {JobID: row.JobID})
MATCH (s:Skill {SkillID: row.SkillID})
MERGE (j)-[:REQUIRES_SKILL]->(s);


// ==============================================================
// 10. Undirected Skill–Skill Relationship (Option A: unique pair)
// ==============================================================
// For each (A,B) pair, create only once based on alphabetical ordering
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Import/skill_related.csv' AS row
WITH row, row.SkillID AS A, row.RelatedSkillID AS B
WHERE A < B
MATCH (s1:Skill {SkillID: A})
MATCH (s2:Skill {SkillID: B})
MERGE (s1)-[:RELATED_TO]->(s2);


// =======================================================
// 11. Relationship: (:Employee)-[:REPORTS_TO]->(:Employee)
// =======================================================
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Import/reports_to.csv' AS row
MATCH (e1:Employee {EmployeeID: row.EmployeeID})
MATCH (e2:Employee {EmployeeID: row.ManagerID})
MERGE (e1)-[:REPORTS_TO]->(e2);


// ======================
// 12. Completion Message
// ======================
RETURN "AURA LOADER COMPLETED SUCCESSFULLY";
