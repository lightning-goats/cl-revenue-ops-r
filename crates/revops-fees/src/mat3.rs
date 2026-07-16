//! 3x3 matrix kernel ported VERBATIM from `fee_controller.py:468-528`
//! (`GaussianThompsonState._mat3_det`, `_mat3_invert`, `_mat3_vec_mul`,
//! `_cholesky3`).
//!
//! Expression shapes and accumulation order are load-bearing: any
//! reassociation changes rounding and can flip the singularity branches
//! (fixture parity is `py_repr`-string equality, not epsilon). Pinned by
//! `fixtures/fees/mat3/{det,invert,matvec,cholesky}.json`, generated from
//! the real Python static methods.

pub type M3 = [[f64; 3]; 3];
pub type V3 = [f64; 3];

/// 3x3 determinant via Sarrus' rule (`_mat3_det`, py 468-472). EXACT
/// expression shape — do not reassociate.
pub fn det3(m: &M3) -> f64 {
    m[0][0] * (m[1][1] * m[2][2] - m[1][2] * m[2][1])
        - m[0][1] * (m[1][0] * m[2][2] - m[1][2] * m[2][0])
        + m[0][2] * (m[1][0] * m[2][1] - m[1][1] * m[2][0])
}

/// 3x3 inverse via cofactor transpose (`_mat3_invert`, py 475-501).
///
/// Returns `None` when `|det| < 1e-10 * max(1.0, max_elem^3)` — a
/// RELATIVE tolerance scaled by the cube of the max element magnitude
/// (py 478-480), with the cube written as three multiplications exactly
/// as Python wrote it.
pub fn invert3(m: &M3) -> Option<M3> {
    let det = det3(m);
    // py 478-479: max(abs(m[i][j]) for i in range(3) for j in range(3))
    let mut max_elem = f64::NEG_INFINITY;
    for row in m {
        for &x in row {
            max_elem = max_elem.max(x.abs());
        }
    }
    let tol = 1e-10 * (max_elem * max_elem * max_elem).max(1.0);
    if det.abs() < tol {
        return None;
    }
    let inv_det = 1.0 / det;
    // Cofactor matrix transposed (py 484-501), element order verbatim.
    Some([
        [
            (m[1][1] * m[2][2] - m[1][2] * m[2][1]) * inv_det,
            (m[0][2] * m[2][1] - m[0][1] * m[2][2]) * inv_det,
            (m[0][1] * m[1][2] - m[0][2] * m[1][1]) * inv_det,
        ],
        [
            (m[1][2] * m[2][0] - m[1][0] * m[2][2]) * inv_det,
            (m[0][0] * m[2][2] - m[0][2] * m[2][0]) * inv_det,
            (m[0][2] * m[1][0] - m[0][0] * m[1][2]) * inv_det,
        ],
        [
            (m[1][0] * m[2][1] - m[1][1] * m[2][0]) * inv_det,
            (m[0][1] * m[2][0] - m[0][0] * m[2][1]) * inv_det,
            (m[0][0] * m[1][1] - m[0][1] * m[1][0]) * inv_det,
        ],
    ])
}

/// 3x3 matrix-vector multiply (`_mat3_vec_mul`, py 504-510): exact
/// `a*b + c*d + e*f` left-to-right shape per component.
pub fn matvec3(m: &M3, v: &V3) -> V3 {
    [
        m[0][0] * v[0] + m[0][1] * v[1] + m[0][2] * v[2],
        m[1][0] * v[0] + m[1][1] * v[1] + m[1][2] * v[2],
        m[2][0] * v[0] + m[2][1] * v[1] + m[2][2] * v[2],
    ]
}

/// 3x3 lower-triangular Cholesky (`_cholesky3`, py 513-528): returns `L`
/// with `L * L^T = m`, or `None` when a diagonal pivot `m[i][i] - s` is
/// `< 1e-12` (absolute) or a divisor `L[j][j] < 1e-12`.
///
/// `s` accumulates `k = 0..j` in ascending order (Python `sum()` order,
/// py 518) — accumulation order is load-bearing.
pub fn cholesky3(m: &M3) -> Option<M3> {
    let mut l: M3 = [[0.0; 3]; 3];
    for i in 0..3 {
        for j in 0..=i {
            let mut s = 0.0;
            // Verbatim py 518 `sum(L[i][k] * L[j][k] for k in range(j))`:
            // the indexed loop mirrors the Python term order exactly.
            #[allow(clippy::needless_range_loop)]
            for k in 0..j {
                s += l[i][k] * l[j][k];
            }
            if i == j {
                let val = m[i][i] - s;
                if val < 1e-12 {
                    return None;
                }
                l[i][j] = val.sqrt();
            } else {
                if l[j][j] < 1e-12 {
                    return None;
                }
                l[i][j] = (m[i][j] - s) / l[j][j];
            }
        }
    }
    Some(l)
}
