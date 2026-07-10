#![allow(clippy::print_stdout, clippy::items_after_statements)]

use std::fmt::Write;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use async_zip::base::write::ZipFileWriter;
use async_zip::{Compression, ZipEntryBuilder};
use criterion::{BatchSize, Criterion};
use futures::executor::block_on;

use uv_distribution_filename::WheelFilename;
use uv_install_wheel::{InstallState, Layout, LinkMode};
use uv_preview::Preview;
use uv_pypi_types::Scheme;

const MANY_FILES_WHEEL_FILENAME: &str = "manyfiles-0.0.0-py3-none-any.whl";
const MANY_FILES_WHEEL_FILE_COUNT: usize = 10_000;

fn create_many_files_wheel() -> tempfile::NamedTempFile {
    let archive = tempfile::NamedTempFile::new().expect("Failed to create temporary archive");
    let mut writer = ZipFileWriter::new(Vec::new());
    let mut record = String::new();
    for index in 0..MANY_FILES_WHEEL_FILE_COUNT {
        // Place files at the top level to ensure register_installed_paths has to read and register 10,000 top-level entries.
        let path = format!("file_{index}.txt");
        write_zip_entry(&mut writer, &path, b"");
        writeln!(record, "{path},,0").expect("Writing to a string cannot fail");
    }
    write_zip_entry(
        &mut writer,
        "manyfiles-0.0.0.dist-info/METADATA",
        b"Metadata-Version: 2.1\nName: manyfiles\nVersion: 0.0.0\n",
    );
    write_zip_entry(
        &mut writer,
        "manyfiles-0.0.0.dist-info/WHEEL",
        b"Wheel-Version: 1.0\nGenerator: uv-bench\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
    );
    record.push_str("manyfiles-0.0.0.dist-info/METADATA,,\n");
    record.push_str("manyfiles-0.0.0.dist-info/WHEEL,,\n");
    record.push_str("manyfiles-0.0.0.dist-info/RECORD,,\n");
    write_zip_entry(
        &mut writer,
        "manyfiles-0.0.0.dist-info/RECORD",
        record.as_bytes(),
    );
    fs_err::write(
        archive.path(),
        block_on(writer.close()).expect("Failed to finish ZIP archive"),
    )
    .expect("Failed to write temporary archive");
    archive
}

fn prepare_wheel(
    archive: fs_err::File,
    extracted_wheel: &Path,
    filename: &WheelFilename,
) -> Vec<(PathBuf, u64)> {
    let files = uv_extract::unzip(archive, extracted_wheel).expect("Failed to extract wheel");
    uv_install_wheel::validate_and_heal_record(extracted_wheel, files.iter(), filename)
        .expect("Failed to validate wheel");
    files
}

fn write_zip_entry(writer: &mut ZipFileWriter<Vec<u8>>, path: &str, contents: &[u8]) {
    let entry = ZipEntryBuilder::new(path.into(), Compression::Stored);
    block_on(writer.write_entry_whole(entry, contents)).expect("Failed to write ZIP entry");
}

fn layout(root: &Path) -> Layout {
    let site_packages = root.join("site-packages");
    Layout {
        sys_executable: root.join("bin/python"),
        python_version: (3, 11),
        os_name: "posix".to_string(),
        scheme: Scheme {
            purelib: site_packages.clone(),
            platlib: site_packages,
            scripts: root.join("bin"),
            data: root.to_path_buf(),
            include: root.join("include"),
        },
    }
}

#[test]
fn test_measure_install_wheel_many_files() {
    let archive = create_many_files_wheel();
    let filename =
        WheelFilename::from_str(MANY_FILES_WHEEL_FILENAME).expect("Invalid wheel filename");
    let extracted_wheel = tempfile::tempdir().expect("Failed to create wheel extraction directory");
    prepare_wheel(
        fs_err::File::open(archive.path()).expect("Failed to open temporary archive"),
        extracted_wheel.path(),
        &filename,
    );

    let mut c = Criterion::default()
        .sample_size(10)
        .measurement_time(std::time::Duration::from_millis(1500))
        .warm_up_time(std::time::Duration::from_millis(500));
    c.bench_function("install_wheel_many_files", |b| {
        b.iter_batched(
            || {
                let environment =
                    tempfile::tempdir().expect("Failed to create installation directory");
                let layout = layout(environment.path());
                fs_err::create_dir_all(&layout.scheme.purelib)
                    .expect("Failed to create site-packages directory");
                (environment, layout)
            },
            |(environment, layout)| {
                let state = InstallState::new(Preview::default());
                uv_install_wheel::install_wheel(
                    &layout,
                    false,
                    extracted_wheel.path(),
                    &filename,
                    None,
                    None::<&()>,
                    None::<&()>,
                    Some("uv"),
                    true,
                    LinkMode::default(),
                    &state,
                )
                .expect("Failed to install wheel");
                state
                    .warn_package_conflicts()
                    .expect("Failed to check for package conflicts");
                black_box((environment, layout))
            },
            BatchSize::SmallInput,
        );
    });
}
