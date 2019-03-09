/*
This file contains:
[Procedure 1]: dijResolve - Resolves the path between any two table (nodes) with defined reference relationship. This uses shortest route algorithm to resolve the path between any two nodes.
*/

DROP PROCEDURE IF EXISTS dijResolve;
DELIMITER | 
CREATE PROCEDURE dijResolve( pFromNodeName VARCHAR(20), pToNodeName VARCHAR(20) ) 
BEGIN 
  DECLARE vFromNodeID, vToNodeID, vNodeID, vCost, vPathID, vFromRouteNodeID, vToRouteNodeID, vReferenceID INT; 
  DECLARE vFromNodeName, vToNodeName VARCHAR(20); 
  -- null out path info in the nodes table 
  UPDATE dijnodes SET PathID = NULL,Cost = NULL,Calculated = 0; 
  -- find nodeIDs referenced by input params 
  SET vFromNodeID = ( SELECT NodeID FROM dijnodes WHERE NodeName = pFromNodeName ); 
  
  IF vFromNodeID IS NULL THEN 
    SELECT CONCAT('From node name ', pFromNodeName, ' not found.' );  
  ELSE 
    BEGIN 
      -- start at src node 
      SET vNodeID = vFromNodeID; 
      SET vToNodeID = ( SELECT NodeID FROM dijnodes WHERE NodeName = pToNodeName ); 
      IF vToNodeID IS NULL THEN 
        SELECT CONCAT('From node name ', pToNodeName, ' not found.' ); 
      ELSE 
        BEGIN 
          -- calculate path costs till all are done 
          UPDATE dijnodes SET Cost=0 WHERE NodeID = vFromNodeID; 
          WHILE vNodeID IS NOT NULL DO 
            BEGIN 
              UPDATE  
                dijnodes AS src 
                JOIN dijpaths AS paths ON paths.FromNodeID = src.NodeID 
                JOIN dijnodes AS dest ON dest.NodeID = paths.ToNodeID 
              SET dest.Cost = CASE 
                                WHEN dest.Cost IS NULL THEN src.Cost + paths.Cost
                                WHEN src.Cost + paths.Cost < dest.Cost THEN src.Cost + paths.Cost 
                                ELSE dest.Cost 
                              END, 
                  dest.PathID = paths.PathID 
              WHERE  
                src.NodeID = vNodeID 
                AND (dest.Cost IS NULL OR src.Cost + paths.Cost < dest.Cost) 
                AND dest.Calculated = 0; 
        
              UPDATE dijnodes SET Calculated = 1 WHERE NodeID = vNodeID; 

              SET vNodeID = ( SELECT NodeID FROM dijnodes 
                              WHERE Calculated = 0 AND Cost IS NOT NULL 
                              ORDER BY Cost LIMIT 1 
                            ); 
            END; 
          END WHILE; 
        END; 
      END IF; 
    END; 
  END IF; 
  IF EXISTS( SELECT 1 FROM dijnodes WHERE NodeID = vToNodeID AND Cost IS NULL ) THEN 
    -- problem,  cannot proceed 
    SELECT CONCAT( 'Node ',vNodeID, ' missed.' ); 
  ELSE 
    BEGIN 
      -- write itinerary to map table 
      DROP TEMPORARY TABLE IF EXISTS map; 
      CREATE TEMPORARY TABLE map ( 
        RowID INT PRIMARY KEY AUTO_INCREMENT, 
        FromRouteNodeID INT, 
        FromNodeName VARCHAR(20), 
        ToRouteNodeID INT,
        ToNodeName VARCHAR(20), 
        Cost INT ,
        RelationReferenceID INT
      ) ENGINE=MEMORY; 
      WHILE vFromNodeID <> vToNodeID DO 
        BEGIN 
          SELECT  
            src.NodeID, src.NodeName,dest.NodeID, dest.NodeName,dest.Cost,dest.PathID , paths.RelationReferenceID
            INTO vFromRouteNodeId, vFromNodeName, vToRouteNodeID, vToNodeName, vCost, vPathID , vReferenceID
          FROM  
            dijnodes AS dest 
            JOIN dijpaths AS paths ON paths.PathID = dest.PathID 
            JOIN dijnodes AS src ON src.NodeID = paths.FromNodeID 
          WHERE dest.NodeID = vToNodeID; 
           
          INSERT INTO map(FromRouteNodeId, FromNodeName,ToRouteNodeID, ToNodeName,Cost,RelationReferenceID  ) VALUES(vFromRouteNodeId,vFromNodeName,vToRouteNodeID,vToNodeName,vCost, vReferenceID); 
          SET vToNodeID = (SELECT FromNodeID FROM dijpaths WHERE PathID = vPathID); 
        END; 
      END WHILE; 
      #SELECT FromRouteNodeID,FromNodeName,ToRouteNodeID,ToNodeName,Cost FROM Map ORDER BY RowID DESC; 
      #DROP TEMPORARY TABLE Map; 
    END; 
  END IF; 
END; 
| 
DELIMITER ; 
