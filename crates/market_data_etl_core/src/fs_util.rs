use anyhow::{Context, Result};
use std::fs;
use std::path::{Path, PathBuf};

pub fn list_files_recursive(root: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    if !root.exists() {
        return Ok(out);
    }
    visit(root, &mut out)?;
    out.sort();
    Ok(out)
}

fn visit(path: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    let metadata = fs::metadata(path).with_context(|| format!("stat {}", path.display()))?;
    if metadata.is_file() {
        out.push(path.to_path_buf());
        return Ok(());
    }
    if metadata.is_dir() {
        let mut entries = fs::read_dir(path)
            .with_context(|| format!("read dir {}", path.display()))?
            .collect::<std::io::Result<Vec<_>>>()
            .with_context(|| format!("collect dir {}", path.display()))?;
        entries.sort_by_key(|entry| entry.path());
        for entry in entries {
            visit(&entry.path(), out)?;
        }
    }
    Ok(())
}

pub fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<()> {
    if dst.exists() {
        fs::remove_dir_all(dst).with_context(|| format!("clear {}", dst.display()))?;
    }
    fs::create_dir_all(dst).with_context(|| format!("create {}", dst.display()))?;
    for file in list_files_recursive(src)? {
        let rel = file
            .strip_prefix(src)
            .with_context(|| format!("strip prefix {} from {}", src.display(), file.display()))?;
        let target = dst.join(rel);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create export parent {}", parent.display()))?;
        }
        fs::copy(&file, &target)
            .with_context(|| format!("copy {} to {}", file.display(), target.display()))?;
    }
    Ok(())
}
