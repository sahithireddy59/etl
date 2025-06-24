import React from 'react';
import { Handle } from 'reactflow';

export default function ExpressionNode({ data }) {
  return (
    <div style={{ padding: 10, border: '1px solid #888', borderRadius: 5, background: '#f8f8ff', minWidth: 180 }}>
      <strong>Expression</strong>
      <ul style={{ paddingLeft: 16 }}>
        {data.expressions && data.expressions.length > 0 ? (
          data.expressions.map((expr, idx) => (
            <li key={idx}><b>{expr.column}</b>: {expr.expression}</li>
          ))
        ) : (
          <li style={{ color: '#aaa' }}>No expressions</li>
        )}
      </ul>
      <Handle type="target" position="top" />
      <Handle type="source" position="bottom" />
    </div>
  );
} 