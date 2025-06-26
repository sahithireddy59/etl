import React, { useCallback, useState, useEffect, useRef } from 'react';
import ReactFlow, {
  MiniMap, Controls, Background, applyNodeChanges, applyEdgeChanges, Handle
} from 'reactflow';
import 'reactflow/dist/style.css';

const CustomNode = (props) => {
  const { data } = props;
  return (
    <div style={{ padding: 10, border: '1px solid #888', borderRadius: 5, background: '#f8f8ff', minWidth: 180 }}>
      <strong>{data.label}</strong>
      {data.type === 'expression' && (
        <ul style={{ paddingLeft: 16 }}>
          {data.expressions && data.expressions.length > 0
            ? data.expressions.map((expr, idx) => (
                <li key={idx}><b>{expr.column}</b>: {expr.expression}</li>
              ))
            : <li style={{ color: '#aaa' }}>No expressions</li>
          }
        </ul>
      )}
      {data.type === 'filter' && (
        <div style={{ color: '#555', marginTop: 6 }}>
          {data.condition || <span style={{ color: '#aaa' }}>No condition</span>}
        </div>
      )}
      {data.type === 'rollup' && (
        <div style={{ fontSize: 13, marginTop: 6 }}>
          <b>Group by:</b> {data.groupBy?.join(', ') || <span style={{ color: '#aaa' }}>None</span>}
          <br />
          <b>Aggregations:</b> {data.aggregations
            ? Object.entries(data.aggregations).map(([col, agg]) => `${agg}(${col})`).join(', ')
            : <span style={{ color: '#aaa' }}>None</span>}
        </div>
      )}
      {data.type === 'joiner' && (
        <div style={{ fontSize: 13, marginTop: 6 }}>
          <b>Join Table:</b> {data.joinTable || <span style={{ color: '#aaa' }}>None</span>}<br />
          <b>Condition:</b> {data.joinCondition || <span style={{ color: '#aaa' }}>None</span>}<br />
          <b>Type:</b> {data.joinType || <span style={{ color: '#aaa' }}>inner</span>}
        </div>
      )}
      <Handle type="target" position="top" />
      <Handle type="source" position="bottom" />
    </div>
  );
};

const nodeTypes = { custom: CustomNode };

const initialNodes = [
  { id: '1', type: 'custom', data: { label: 'Source Table', type: 'input' }, position: { x: 0, y: 100 } },
  { id: '2', type: 'custom', data: { label: 'Target Table', type: 'output' }, position: { x: 500, y: 100 } },
];

const initialEdges = [];

export default function App() {
  const [nodes, setNodes] = useState(initialNodes);
  const [edges, setEdges] = useState(initialEdges);
  const [selectedNode, setSelectedNode] = useState(null);
  const [labelInput, setLabelInput] = useState('');
  const [expressions, setExpressions] = useState([]);
  const [filterCondition, setFilterCondition] = useState('');
  const [groupBy, setGroupBy] = useState([]);
  const [aggregations, setAggregations] = useState({});
  const [showHelp, setShowHelp] = useState(false);
  const [showSimpleCopy, setShowSimpleCopy] = useState(false);
  const [simpleCopyName, setSimpleCopyName] = useState('');
  const [simpleCopySource, setSimpleCopySource] = useState('');
  const [simpleCopyTarget, setSimpleCopyTarget] = useState('');
  const [aggInput, setAggInput] = useState('');
  const [joinTable, setJoinTable] = useState('');
  const [joinCondition, setJoinCondition] = useState('');
  const [joinType, setJoinType] = useState('inner');
  const [leftTable, setLeftTable] = useState('');
  const [rightTable, setRightTable] = useState('');
  const [leftKey, setLeftKey] = useState('');
  const [rightKey, setRightKey] = useState('');
  const [joinerFields, setJoinerFields] = useState([]);
  const [leftTableColumns, setLeftTableColumns] = useState([]);
  const [rightTableColumns, setRightTableColumns] = useState([]);
  const [loadingLeftCols, setLoadingLeftCols] = useState(false);
  const [loadingRightCols, setLoadingRightCols] = useState(false);
  const [dialogPos, setDialogPos] = useState({ top: '25%', left: '35%' });
  const dragOffset = useRef({ x: 0, y: 0 });
  const dragging = useRef(false);
  const [leftTableError, setLeftTableError] = useState('');
  const [rightTableError, setRightTableError] = useState('');
  const [fieldNameMap, setFieldNameMap] = useState({});

  // Add node functions
  const addExpressionNode = () => {
    const newId = (nodes.length + 1).toString();
    setNodes(nds => [
      ...nds,
      {
        id: newId,
        type: 'custom',
        data: { label: 'Expression', type: 'expression', expressions: [] },
        position: { x: 200 + nds.length * 40, y: 200 },
      },
    ]);
  };
  const addFilterNode = () => {
    const newId = (nodes.length + 1).toString();
    setNodes(nds => [
      ...nds,
      {
        id: newId,
        type: 'custom',
        data: { label: 'Filter', type: 'filter', condition: '' },
        position: { x: 200 + nds.length * 40, y: 300 },
      },
    ]);
  };
  const addRollupNode = () => {
    const newId = (nodes.length + 1).toString();
    setNodes(nds => [
      ...nds,
      {
        id: newId,
        type: 'custom',
        data: { label: 'Rollup', type: 'rollup', groupBy: [], aggregations: {} },
        position: { x: 200 + nds.length * 40, y: 400 },
      },
    ]);
  };
  const addJoinerNode = () => {
    const newId = (nodes.length + 1).toString();
    setNodes(nds => [
      ...nds,
      {
        id: newId,
        type: 'custom',
        data: {
          label: 'Joiner',
          type: 'joiner',
          leftTable: '',
          rightTable: '',
          leftKey: '',
          rightKey: '',
          joinType: 'inner',
          selectedFields: [],
        },
        position: { x: 200 + nds.length * 40, y: 500 },
      },
    ]);
  };
  const addSourceTableNode = () => {
    const newId = (nodes.length + 1).toString();
    setNodes(nds => [
      ...nds,
      {
        id: newId,
        type: 'custom',
        data: { label: 'Source Table', type: 'input' },
        position: { x: 0, y: 100 + nds.length * 40 },
      },
    ]);
  };

  const onNodesChange = useCallback(
    (changes) => setNodes((nds) => applyNodeChanges(changes, nds)),
    []
  );
  const onEdgesChange = useCallback(
    (changes) => setEdges((eds) => applyEdgeChanges(changes, eds)),
    []
  );
  const onConnect = useCallback(
    (params) => setEdges((eds) => [...eds, { ...params, id: `${params.source}-${params.target}` }]),
    []
  );
  const onEdgeClick = (event, edge) => {
    event.stopPropagation();
    setEdges(eds => eds.filter(e => e.id !== edge.id));
  };

  // Double-click node to edit
  const onNodeDoubleClick = (event, node) => {
    setSelectedNode(node);
    setLabelInput(node.data.label);
    if (node.data.type === 'expression') {
      setExpressions(node.data.expressions || []);
    } else if (node.data.type === 'filter') {
      setFilterCondition(node.data.condition || '');
    } else if (node.data.type === 'rollup') {
      setGroupBy(node.data.groupBy || []);
      setAggregations(node.data.aggregations || {});
      setAggInput(
        node.data.aggregations
          ? Object.entries(node.data.aggregations).map(([col, agg]) => `${col}:${agg}`).join(',')
          : ''
      );
    } else if (node.data.type === 'joiner') {
      setLeftTable(node.data.leftTable || '');
      setRightTable(node.data.rightTable || '');
      setLeftKey(node.data.leftKey || '');
      setRightKey(node.data.rightKey || '');
      setJoinType(node.data.joinType || 'inner');
      setJoinerFields(node.data.selectedFields || []);
    }
  };

  // Save node edits
  const handleSave = () => {
    let aggs = aggregations;
    if (selectedNode.data.type === 'rollup') {
      // Parse aggInput on save
      aggs = {};
      aggInput.split(',').forEach(pair => {
        const [col, agg] = pair.split(':').map(s => s.trim());
        if (col && agg) aggs[col] = agg;
      });
    }
    setNodes(nds =>
      nds.map(n =>
        n.id === selectedNode.id
          ? {
              ...n,
              data: {
                ...n.data,
                label: labelInput,
                ...(selectedNode.data.type === 'expression' ? { expressions } : {}),
                ...(selectedNode.data.type === 'filter' ? { condition: filterCondition } : {}),
                ...(selectedNode.data.type === 'rollup' ? { groupBy, aggregations: aggs } : {}),
                ...(selectedNode.data.type === 'joiner' ? {
                  leftTable, rightTable, leftKey, rightKey, joinType, selectedFields: joinerFields, fieldNameMap
                } : {}),
              },
            }
          : n
      )
    );
    setSelectedNode(null);
  };

  // Delete node and rewire edges
  const deleteNodeAndRewire = (nodeId) => {
    setNodes(nds => nds.filter(n => n.id !== nodeId));
    setEdges(eds => {
      const incoming = eds.filter(e => e.target === nodeId);
      const outgoing = eds.filter(e => e.source === nodeId);
      let newEdges = eds.filter(e => e.source !== nodeId && e.target !== nodeId);
      incoming.forEach(inEdge => {
        outgoing.forEach(outEdge => {
          if (inEdge.source !== outEdge.target && !newEdges.some(e => e.source === inEdge.source && e.target === outEdge.target)) {
            newEdges.push({
              id: `e${inEdge.source}-${outEdge.target}`,
              source: inEdge.source,
              target: outEdge.target,
            });
          }
        });
      });
      return newEdges;
    });
    setSelectedNode(null);
  };

  // Expression editing helpers
  const handleExprChange = (idx, field, value) => {
    setExpressions(exprs => {
      const updated = [...exprs];
      updated[idx][field] = value;
      return updated;
    });
  };
  const addExprRow = () => setExpressions(exprs => [...exprs, { column: '', expression: '' }]);
  const removeExprRow = idx => setExpressions(exprs => exprs.filter((_, i) => i !== idx));

  const savePipeline = async () => {
    const pipeline = { nodes, edges };
    try {
      const response = await fetch('http://localhost:8000/create/', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(pipeline),
      });
      if (response.ok) {
        alert('Pipeline saved and Airflow triggered!');
      } else {
        alert('Failed to save pipeline.');
      }
    } catch (error) {
      alert('Error: ' + error);
    }
  };

  const handleSimpleCopy = async () => {
    if (!simpleCopyName || !simpleCopySource || !simpleCopyTarget) {
      alert('Please fill all fields.');
      return;
    }
    const payload = {
      name: simpleCopyName,
      nodes: [
        { id: '1', type: 'custom', data: { type: 'input', label: simpleCopySource } },
        { id: '2', type: 'custom', data: { type: 'output', label: simpleCopyTarget } }
      ],
      edges: []
    };
    try {
      const response = await fetch('http://localhost:8000/create/', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      if (response.ok) {
        alert('Simple copy job saved and Airflow triggered!');
        setShowSimpleCopy(false);
        setSimpleCopyName('');
        setSimpleCopySource('');
        setSimpleCopyTarget('');
      } else {
        alert('Failed to save simple copy job.');
      }
    } catch (error) {
      alert('Error: ' + error);
    }
  };

  // Helper: get all input/source nodes
  const sourceNodes = nodes.filter(n => n.data.type === 'input');

  // Helper: get fields for a node (simulate for now)
  const getFieldsForNode = (nodeId) => {
    // In a real app, fetch schema from backend or cache
    // For now, simulate with example fields
    if (nodeId === '1') return ['CustomerID', 'Name', 'Region'];
    if (nodeId === '2') return ['CustomerID', 'OrderID', 'Amount'];
    return ['id', 'col1', 'col2'];
  };

  // When leftTable/rightTable changes, update available fields
  useEffect(() => {
    setLeftTableColumns(leftTable ? getFieldsForNode(leftTable) : []);
  }, [leftTable]);
  useEffect(() => {
    setRightTableColumns(rightTable ? getFieldsForNode(rightTable) : []);
  }, [rightTable]);

  // Helper to get the label (table name) for a given node ID
  const getNodeLabelById = (id) => {
    const node = nodes.find(n => n.id === id);
    return node ? node.data.label : '';
  };

  useEffect(() => {
    if (leftTable) {
      setLoadingLeftCols(true);
      setLeftTableError('');
      const tableName = getNodeLabelById(leftTable);
      fetch(`http://localhost:8000/api/table-columns/?table=${encodeURIComponent(tableName)}`)
        .then(res => res.json())
        .then(data => {
          if (data.error) {
            setLeftTableColumns([]);
            setLeftTableError(data.error);
          } else {
            setLeftTableColumns(data.columns || []);
            setLeftTableError('');
          }
          setLoadingLeftCols(false);
        })
        .catch((err) => {
          setLeftTableColumns([]);
          setLeftTableError('Fetch error');
          setLoadingLeftCols(false);
        });
    } else {
      setLeftTableColumns([]);
      setLeftTableError('');
    }
  }, [leftTable, nodes]);

  useEffect(() => {
    if (rightTable) {
      setLoadingRightCols(true);
      setRightTableError('');
      const tableName = getNodeLabelById(rightTable);
      fetch(`http://localhost:8000/api/table-columns/?table=${encodeURIComponent(tableName)}`)
        .then(res => res.json())
        .then(data => {
          if (data.error) {
            setRightTableColumns([]);
            setRightTableError(data.error);
          } else {
            setRightTableColumns(data.columns || []);
            setRightTableError('');
          }
          setLoadingRightCols(false);
        })
        .catch((err) => {
          setRightTableColumns([]);
          setRightTableError('Fetch error');
          setLoadingRightCols(false);
        });
    } else {
      setRightTableColumns([]);
      setRightTableError('');
    }
  }, [rightTable, nodes]);

  // When joinerFields changes, update fieldNameMap to include new fields with default names
  useEffect(() => {
    setFieldNameMap(prev => {
      const updated = { ...prev };
      joinerFields.forEach(f => {
        if (!updated[f]) updated[f] = f;
      });
      // Remove fields no longer selected
      Object.keys(updated).forEach(f => {
        if (!joinerFields.includes(f)) delete updated[f];
      });
      return updated;
    });
  }, [joinerFields]);

  const handleDialogMouseDown = (e) => {
    dragging.current = true;
    const dialog = document.getElementById('edit-node-dialog');
    const rect = dialog.getBoundingClientRect();
    dragOffset.current = {
      x: e.clientX - rect.left,
      y: e.clientY - rect.top,
    };
    document.body.style.userSelect = 'none';
  };
  const handleDialogMouseMove = (e) => {
    if (!dragging.current) return;
    setDialogPos({
      top: Math.max(0, e.clientY - dragOffset.current.y) + 'px',
      left: Math.max(0, e.clientX - dragOffset.current.x) + 'px',
    });
  };
  const handleDialogMouseUp = () => {
    dragging.current = false;
    document.body.style.userSelect = '';
  };
  useEffect(() => {
    window.addEventListener('mousemove', handleDialogMouseMove);
    window.addEventListener('mouseup', handleDialogMouseUp);
    return () => {
      window.removeEventListener('mousemove', handleDialogMouseMove);
      window.removeEventListener('mouseup', handleDialogMouseUp);
    };
  }, []);

  return (
    <div>
      <div style={{ height: 500 }}>
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onConnect={onConnect}
          onEdgeClick={onEdgeClick}
          onNodeDoubleClick={onNodeDoubleClick}
          fitView
          nodeTypes={nodeTypes}
        >
          <MiniMap />
          <Controls />
          <Background />
        </ReactFlow>
        <div style={{ marginTop: 20 }}>
          <button onClick={addExpressionNode} style={{ marginRight: 10 }}>Add Expression Node</button>
          <button onClick={addFilterNode} style={{ marginRight: 10 }}>Add Filter Node</button>
          <button onClick={addRollupNode} style={{ marginRight: 10 }}>Add Rollup Node</button>
          <button onClick={addJoinerNode} style={{ marginRight: 10 }}>Add Joiner Node</button>
          <button onClick={addSourceTableNode} style={{ marginRight: 10, background: '#e7f7ff' }}>Add Source Table Node</button>
          <button onClick={savePipeline} style={{ marginLeft: 10 }}>Save Pipeline</button>
          <button onClick={() => setShowSimpleCopy(true)} style={{ marginLeft: 10, background: '#2ecc40', color: '#fff' }}>Simple Copy</button>
          <button onClick={() => setShowHelp(h => !h)} style={{ marginLeft: 10 }}>Help</button>
          <span style={{ marginLeft: 20, color: '#888' }}>
            You can add and connect nodes by dragging from the bottom handle to the top handle of another node.
          </span>
        </div>
        {/* Simple Copy Dialog */}
        {showSimpleCopy && (
          <div style={{ position: 'fixed', top: '30%', left: '35%', background: '#fff', padding: 24, border: '1px solid #888', zIndex: 20, minWidth: 400 }}>
            <h3>Simple Table Copy</h3>
            <div style={{ marginBottom: 10 }}>
              <label>Job Name:</label>
              <input value={simpleCopyName} onChange={e => setSimpleCopyName(e.target.value)} style={{ width: '100%' }} />
            </div>
            <div style={{ marginBottom: 10 }}>
              <label>Source Table:</label>
              <input value={simpleCopySource} onChange={e => setSimpleCopySource(e.target.value)} style={{ width: '100%' }} />
            </div>
            <div style={{ marginBottom: 10 }}>
              <label>Target Table:</label>
              <input value={simpleCopyTarget} onChange={e => setSimpleCopyTarget(e.target.value)} style={{ width: '100%' }} />
            </div>
            <div style={{ marginTop: 16 }}>
              <button onClick={handleSimpleCopy} style={{ marginRight: 10 }}>Save</button>
              <button onClick={() => setShowSimpleCopy(false)}>Cancel</button>
            </div>
          </div>
        )}
        {/* Node editing dialog */}
        {selectedNode && (
          <div
            id="edit-node-dialog"
            style={{
              position: 'fixed',
              top: dialogPos.top,
              left: dialogPos.left,
              background: '#fff',
              padding: 24,
              border: '1px solid #888',
              zIndex: 20,
              minWidth: 400,
              maxHeight: '80vh',
              overflowY: 'auto',
              boxShadow: '0 4px 24px rgba(0,0,0,0.15)',
              cursor: 'default',
            }}
          >
            <div
              style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between',
                marginBottom: 8,
                cursor: 'move',
                userSelect: 'none',
              }}
              onMouseDown={handleDialogMouseDown}
            >
              <h3 style={{ margin: 0, fontWeight: 500, fontSize: 20 }}>Edit Node</h3>
              <div>
                {selectedNode.data.type !== 'input' && selectedNode.data.type !== 'output' && (
                  <button
                    onClick={() => { deleteNodeAndRewire(selectedNode.id); setSelectedNode(null); }}
                    style={{
                      background: 'transparent',
                      border: 'none',
                      fontSize: 20,
                      color: '#d00',
                      cursor: 'pointer',
                      marginRight: 8,
                    }}
                    aria-label="Delete Node"
                    title="Delete Node"
                  >
                    🗑️
                  </button>
                )}
                <button
                  onClick={() => setSelectedNode(null)}
                  style={{
                    background: 'transparent',
                    border: 'none',
                    fontSize: 22,
                    cursor: 'pointer',
                    color: '#888',
                  }}
                  aria-label="Close"
                >
                  ×
                </button>
              </div>
            </div>
            <div>
              <label>Label:</label>
              <input
                value={labelInput}
                onChange={e => setLabelInput(e.target.value)}
                style={{ width: '100%', marginBottom: 10 }}
              />
            </div>
            {selectedNode.data.type === 'expression' && (
              <div>
                <h4>Expressions</h4>
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
              </div>
            )}
            {selectedNode.data.type === 'filter' && (
              <div>
                <label>Condition:</label>
                <input
                  placeholder="e.g. count_st > 10"
                  value={filterCondition}
                  onChange={e => setFilterCondition(e.target.value)}
                  style={{ width: '100%', marginBottom: 10 }}
                />
              </div>
            )}
            {selectedNode.data.type === 'rollup' && (
              <div>
                <label>Group By (comma separated):</label>
                <input
                  value={groupBy.join(',')}
                  onChange={e => setGroupBy(e.target.value.split(',').map(s => s.trim()).filter(Boolean))}
                  style={{ width: '100%', marginBottom: 10 }}
                />
                <label>Aggregations (format: col:agg, e.g. count_st:sum):</label>
                <input
                  value={aggInput}
                  onChange={e => setAggInput(e.target.value)}
                  style={{ width: '100%', marginBottom: 10 }}
                />
              </div>
            )}
            {selectedNode.data.type === 'joiner' && (
              <div>
                <label>Left Table:</label>
                <select value={leftTable} onChange={e => setLeftTable(e.target.value)} style={{ width: '100%', marginBottom: 10 }}>
                  <option value=''>Select...</option>
                  {sourceNodes.map(n => <option key={n.id} value={n.id}>{n.data.label || n.id}</option>)}
                </select>
                <label>Right Table:</label>
                <select value={rightTable} onChange={e => setRightTable(e.target.value)} style={{ width: '100%', marginBottom: 10 }}>
                  <option value=''>Select...</option>
                  {sourceNodes.map(n => <option key={n.id} value={n.id}>{n.data.label || n.id}</option>)}
                </select>
                <label>Left Join Key:</label>
                <select value={leftKey} onChange={e => setLeftKey(e.target.value)} style={{ width: '100%', marginBottom: 10 }}>
                  <option value=''>Select...</option>
                  {loadingLeftCols ? <option>Loading...</option> : leftTableColumns.map(f => <option key={f} value={f}>{f}</option>)}
                </select>
                <label>Right Join Key:</label>
                <select value={rightKey} onChange={e => setRightKey(e.target.value)} style={{ width: '100%', marginBottom: 10 }}>
                  <option value=''>Select...</option>
                  {loadingRightCols ? <option>Loading...</option> : rightTableColumns.map(f => <option key={f} value={f}>{f}</option>)}
                </select>
                <label>Join Type:</label>
                <select value={joinType} onChange={e => setJoinType(e.target.value)} style={{ width: '100%', marginBottom: 10 }}>
                  <option value="inner">Inner</option>
                  <option value="left">Left</option>
                  <option value="right">Right</option>
                  <option value="outer">Full Outer</option>
                  <option value="cross">Cross</option>
                </select>
                <label>Fields to include in output:</label>
                <select multiple value={joinerFields} onChange={e => setJoinerFields(Array.from(e.target.selectedOptions, o => o.value))} style={{ width: '100%', marginBottom: 10, height: 80 }}>
                  {leftTableColumns.map(f => <option key={'l_' + f} value={f}>{`L: ${f}`}</option>)}
                  {rightTableColumns.map(f => <option key={'r_' + f} value={f + '_r'}>{`R: ${f}`}</option>)}
                </select>
                {joinerFields.length > 0 && (
                  <div style={{ marginTop: 12 }}>
                    <label style={{ display: 'block' }}>Customize Output Column Names:</label>
                    {joinerFields.map(f => (
                      <div key={f} style={{ display: 'flex', alignItems: 'center', marginBottom: 6 }}>
                        <span style={{ minWidth: 80 }}>{f}</span>
                        <input
                          value={fieldNameMap[f] || ''}
                          onChange={e => setFieldNameMap(m => ({ ...m, [f]: e.target.value }))}
                          style={{ marginLeft: 8, width: 180 }}
                          placeholder={f}
                        />
                      </div>
                    ))}
                  </div>
                )}
              </div>
            )}
            <div style={{ marginTop: 16 }}>
              <button onClick={handleSave}>Save</button>
              <button onClick={() => setSelectedNode(null)} style={{ marginLeft: 10 }}>Cancel</button>
            </div>
          </div>
        )}
        {/* Help overlay */}
        {showHelp && (
          <div style={{ position: 'fixed', top: 60, left: '20%', width: 400, background: '#fff', border: '2px solid #0074D9', borderRadius: 8, padding: 20, zIndex: 1000 }}>
            <h3>How to use the ETL Visual Builder</h3>
            <ul>
              <li>To <b>connect nodes</b>: Drag from the <b>bottom handle</b> of one node to the <b>top handle</b> of another node.</li>
              <li>To <b>delete a connection</b>: Click on the link/edge between nodes.</li>
              <li>To <b>add a node</b>: Use the buttons below the canvas.</li>
              <li>To <b>edit or delete a node</b>: Double-click the node.</li>
              <li>To <b>save your pipeline</b>: Click "Save Pipeline".</li>
            </ul>
            <button onClick={() => setShowHelp(false)} style={{ marginTop: 10 }}>Close</button>
          </div>
        )}
      </div>
    </div>
  );
}