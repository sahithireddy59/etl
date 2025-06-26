import React from 'react';
import { Handle } from 'reactflow';

export default function RollupNode({ data }) {
  return (
    <div style={{ padding: 10, border: '1px solid #888', borderRadius: 5, background: '#e7f7ff', minWidth: 180 }}>
      <strong>Rollup</strong>
      <div style={{ fontSize: 13, marginTop: 6 }}>
        <b>Group by:</b> {data.groupBy?.join(', ') || <span style={{ color: '#aaa' }}>None</span>}
        <br />
        <b>Aggregations:</b> {data.aggregations ? Object.entries(data.aggregations).map(([col, agg]) => `${agg}(${col})`).join(', ') : <span style={{ color: '#aaa' }}>None</span>}
      </div>
      <Handle type="target" position="top" />
      <Handle type="source" position="bottom" />
    </div>
  );
} 