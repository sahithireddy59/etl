import React, { useCallback, useState, useEffect } from 'react';
import ReactFlow, {
  MiniMap, Controls, Background, addEdge, useNodesState, useEdgesState
} from 'reactflow';
import 'reactflow/dist/style.css';
import ExpressionNode from './ExpressionNode';

const initialNodes = [
  { id: '1', type: 'input', data: { label: 'Source Table' }, position: { x: 0, y: 100 } },
  { id: '2', type: 'expression', data: { expressions: [] }, position: { x: 250, y: 100 } },
  { id: '3', type: 'output', data: { label: 'Target Table' }, position: { x: 500, y: 100 } },
];

const initialEdges = [
  { id: 'e1-2', source: '1', target: '2' },
  { id: 'e2-3', source: '2', target: '3' },
];

const nodeTypes = {
  expression: ExpressionNode,
};

function App() {
  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);
  const [jobs, setJobs] = useState([]);

  const [selectedNode, setSelectedNode] = useState(null);
  const [labelInput, setLabelInput] = useState('');
  const [expressions, setExpressions] = useState([]);

  const onConnect = useCallback(
    (params) => setEdges((eds) => addEdge(params, eds)),
    [setEdges]
  );

  // Fetch jobs from backend
  const fetchJobs = () => {
    fetch('http://localhost:8000/api/jobs/')
      .then(res => res.json())
      .then(data => setJobs(data.jobs));
  };

  useEffect(() => {
    fetchJobs();
  }, []);

  // Double-click node to edit
  const onNodeDoubleClick = (event, node) => {
    if (node.type === 'expression') {
      setSelectedNode(node);
      setExpressions(node.data.expressions || []);
    } else {
      setSelectedNode(node);
      setLabelInput(node.data.label);
    }
  };

  // Save node label
  const handleSave = () => {
    setNodes((nds) =>
      nds.map((n) =>
        n.id === selectedNode.id ? { ...n, data: { ...n.data, label: labelInput } } : n
      )
    );
    setSelectedNode(null);
  };

  // Save expressions for Expression node
  const handleSaveExpressions = () => {
    setNodes((nds) =>
      nds.map((n) =>
        n.id === selectedNode.id ? { ...n, data: { ...n.data, expressions } } : n
      )
    );
    setSelectedNode(null);
    setExpressions([]);
  };

  // Add new Expression node
  const addExpressionNode = () => {
    const newId = (nodes.length + 1).toString();
    setNodes((nds) => [
      ...nds,
      {
        id: newId,
        type: 'expression',
        data: { expressions: [] },
        position: { x: 200 + nds.length * 40, y: 200 },
      },
    ]);
  };

  // Add/remove/edit expressions in dialog
  const handleExprChange = (idx, field, value) => {
    setExpressions((exprs) => {
      const updated = [...exprs];
      updated[idx][field] = value;
      return updated;
    });
  };
  const addExprRow = () => setExpressions((exprs) => [...exprs, { column: '', expression: '' }]);
  const removeExprRow = (idx) => setExpressions((exprs) => exprs.filter((_, i) => i !== idx));

  // Example: Save pipeline as JSON
  const savePipeline = async () => {
    // Deep copy nodes to avoid mutating state
    const newNodes = nodes.map(node => {
      if (node.type === 'expression' && node.data && Array.isArray(node.data.expressions)) {
        const newExpressions = node.data.expressions.map(expr => {
          // If expression is just a function name, rewrite as FUNCTION(column)
          if (expr.expression && expr.column && /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(expr.expression)) {
            return {
              ...expr,
              expression: `${expr.expression}(${expr.column})`
            };
          }
          return expr;
        });
        return { ...node, data: { ...node.data, expressions: newExpressions } };
      }
      return node;
    });
    const pipeline = { nodes: newNodes, edges };
    try {
      const response = await fetch('http://localhost:8000/create/', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(pipeline),
      });
      if (response.ok) {
        alert('Pipeline saved and Airflow triggered!');
        fetchJobs(); // Refresh jobs after save
      } else {
        alert('Failed to save pipeline.');
      }
    } catch (error) {
      alert('Error: ' + error);
    }
  };

  // Data viewing state
  const [tableData, setTableData] = useState(null);
  const [tableColumns, setTableColumns] = useState([]);
  const [showDataModal, setShowDataModal] = useState(false);
  const [dataTableName, setDataTableName] = useState('');

  // Fetch data for a job's target table
  const viewData = async (targetTable) => {
    try {
      const res = await fetch(`http://localhost:8000/api/table_data/?table=${targetTable}`);
      const data = await res.json();
      setTableColumns(data.columns);
      setTableData(data.rows);
      setDataTableName(targetTable);
      setShowDataModal(true);
    } catch (err) {
      alert('Failed to fetch table data');
    }
  };

  return (
    <div>
      <div style={{ height: 500 }}>
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onConnect={onConnect}
          onNodeDoubleClick={onNodeDoubleClick}
          fitView
          nodeTypes={nodeTypes}
        >
          <MiniMap />
          <Controls />
          <Background />
        </ReactFlow>
        <button onClick={addExpressionNode} style={{ marginTop: 20, marginRight: 10 }}>Add Expression Node</button>
        <button onClick={savePipeline} style={{ marginTop: 20 }}>Save Pipeline</button>

        {/* Node configuration dialog */}
        {selectedNode && selectedNode.type !== 'expression' && (
          <div style={{
            position: 'fixed', top: '30%', left: '40%', background: '#fff', padding: 20, border: '1px solid #ccc', zIndex: 10
          }}>
            <h3>Edit Node</h3>
            <input
              value={labelInput}
              onChange={e => setLabelInput(e.target.value)}
              style={{ width: '100%', marginBottom: 10 }}
            />
            <br />
            <button onClick={handleSave}>Save</button>
            <button onClick={() => setSelectedNode(null)} style={{ marginLeft: 10 }}>Cancel</button>
          </div>
        )}
        {/* Expression node dialog */}
        {selectedNode && selectedNode.type === 'expression' && (
          <div style={{
            position: 'fixed', top: '25%', left: '35%', background: '#fff', padding: 24, border: '1px solid #888', zIndex: 20, minWidth: 400
          }}>
            <h3>Edit Expressions</h3>
            {expressions.map((expr, idx) => (
              <div key={idx} style={{ display: 'flex', gap: 8, marginBottom: 6 }}>
                <input
                  placeholder="Output Column"
                  value={expr.column}
                  onChange={e => handleExprChange(idx, 'column', e.target.value)}
                  style={{ width: 120 }}
                />
                <input
                  placeholder="Expression"
                  value={expr.expression}
                  onChange={e => handleExprChange(idx, 'expression', e.target.value)}
                  style={{ width: 200 }}
                />
                <button onClick={() => removeExprRow(idx)} style={{ color: 'red' }}>Remove</button>
              </div>
            ))}
            <button onClick={addExprRow} style={{ marginTop: 8 }}>Add Expression</button>
            <div style={{ marginTop: 16 }}>
              <button onClick={handleSaveExpressions}>Save</button>
              <button onClick={() => setSelectedNode(null)} style={{ marginLeft: 10 }}>Cancel</button>
            </div>
          </div>
        )}
      </div>
      <h2>ETL Jobs</h2>
      <table border="1" cellPadding="6" style={{ marginTop: 20, borderCollapse: 'collapse' }}>
        <thead>
          <tr>
            <th>ID</th>
            <th>Source</th>
            <th>Target</th>
            <th>Rule</th>
            <th>Status</th>
            <th>Created</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          {jobs.map(job => (
            <tr key={job.id}>
              <td>{job.id}</td>
              <td>{job.source_table}</td>
              <td>{job.target_table}</td>
              <td>{job.transformation_rule}</td>
              <td>{job.status || 'unknown'}</td>
              <td>{job.created_at}</td>
              <td><button onClick={() => viewData(job.target_table)}>View Data</button></td>
            </tr>
          ))}
        </tbody>
      </table>
      {/* Data Modal */}
      {showDataModal && (
        <div style={{
          position: 'fixed', top: 0, left: 0, width: '100vw', height: '100vh', background: 'rgba(0,0,0,0.5)', zIndex: 1000,
          display: 'flex', alignItems: 'center', justifyContent: 'center'
        }}>
          <div style={{ background: 'white', padding: 20, borderRadius: 8, maxHeight: '80vh', overflow: 'auto' }}>
            <button style={{ float: 'right', marginBottom: 10 }} onClick={() => setShowDataModal(false)}>Close</button>
            <h3>Table Data: {dataTableName}</h3>
            {tableColumns.length === 0 ? (
              <div>No data found.</div>
            ) : (
              <table border="1" cellPadding="5" style={{ borderCollapse: 'collapse', width: '100%' }}>
                <thead>
                  <tr>
                    {tableColumns.map(col => <th key={col}>{col}</th>)}
                  </tr>
                </thead>
                <tbody>
                  {tableData && tableData.map((row, i) => (
                    <tr key={i}>
                      {row.map((cell, j) => <td key={j}>{cell}</td>)}
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

export default App;