from datetime import datetime, timezone

import pandas
from dagster import solid, OutputDefinition, String, AssetMaterialization, EventMetadataEntry, Output, \
    InputDefinition, Field, Dict, composite_solid, List, AssetKey, Any
from dagster_ge import ge_validation_solid_factory
from dagster_pandas import create_dagster_pandas_dataframe_type, PandasColumn


def compute_releases_dataframe_summary_statistics(dataframe):
    return [
        EventMetadataEntry.md(dataframe.head(5).to_markdown(), "head(5)"),
        EventMetadataEntry.int(len(dataframe), "n_rows"),
        EventMetadataEntry.text(str(dataframe.columns), "columns"),
    ]


ReleasesDataFrame = create_dagster_pandas_dataframe_type(
    name="ReleasesDataFrame",
    columns=[
        PandasColumn.string_column("product_id", non_nullable=True, is_required=True),
        PandasColumn.string_column("version", non_nullable=True, is_required=True),
        PandasColumn.string_column("name", non_nullable=True, is_required=True),
        PandasColumn.datetime_column("release_date", non_nullable=True, is_required=True),
        PandasColumn.string_column("link", non_nullable=True, is_required=True),
    ],
    event_metadata_fn=compute_releases_dataframe_summary_statistics,
)


@solid()
def join_releases(_, release_list: List[ReleasesDataFrame]) -> ReleasesDataFrame:
    return pandas.concat(release_list)


@solid(
    description="Persists releases to database (overwriting any existing data)",
    input_defs=[
        InputDefinition(
            name="releases", dagster_type=ReleasesDataFrame,
            description="Releases to persist")
    ],
    output_defs=[OutputDefinition(name="asset_path", dagster_type=String)],
    required_resource_keys={"datawarehouse"},
    tags={"kind": "add_to_lake"},
)
def add_releases_to_lake(context, releases):
    datalake_uri = context.resources.datawarehouse.datalake_uri
    at_date = datetime.now(timezone.utc).date()

    asset_path = context.resources.datawarehouse.add_to_lake(releases, "release", at_date)

    yield AssetMaterialization(
        asset_key=AssetKey([datalake_uri, "release"]),
        metadata_entries=[
            EventMetadataEntry.text(datalake_uri, "datalake_uri"),
            EventMetadataEntry.text("release", "asset_type"),
            EventMetadataEntry.text(at_date.strftime("%Y-%m-%d"), "partition_key"),
            EventMetadataEntry.text(asset_path, "asset_path"),
            EventMetadataEntry.int(releases.shape[0], "n_rows"),
            EventMetadataEntry.md(releases.head(5).to_markdown(), "head(5)"),
        ],
    )
    yield Output(value=asset_path, output_name="asset_path")


ge_releases_validation = ge_validation_solid_factory(
    name="validate_releases_expectations",
    datasource_name="dagster_datasource",
    suite_name="software_releases_lake.releases",
    validation_operator_name="action_list_operator",
    input_dagster_type=ReleasesDataFrame,
)


@solid(
    tags={"kind": "raise_on_failure"},
)
def raise_on_failure(_, releases: ReleasesDataFrame, expectation_result: Dict) -> ReleasesDataFrame:
    if expectation_result["success"]:
        return releases
    else:
        raise ValueError


@composite_solid()
def validate_releases(releases: ReleasesDataFrame) -> ReleasesDataFrame:
    return raise_on_failure(releases, ge_releases_validation(releases))


@solid()
def make_asset(_) -> List[Any]:
    yield AssetMaterialization(
        asset_key=AssetKey(["datalake", "my_asset"]),
        metadata_entries=[
            EventMetadataEntry.path("file:///path/to/file.csv", "path_key", "This is the description"),
            EventMetadataEntry.text("text value", "text_key", "This is the description"),
            EventMetadataEntry.int(42, "int_key", description="This is the description"),
            EventMetadataEntry.float(42.123, "float_key"),
            EventMetadataEntry.url("https://somewhere.com", "url_key"),
            EventMetadataEntry.md(pandas.DataFrame(data={'col1': [1, 2], 'col2': [3, 4]}).to_markdown(), "pandas.to_markdown"),
            EventMetadataEntry.md("""
### Some markdown

| Item                         	    | Value  	|
|----------------------------------:|----------:|
| Pomodoros done                   	| 0        	|
| Hours in meetings / Hours worked 	| 0/0      	|
            """, "markdown_key"),
            EventMetadataEntry.md("""
### Some MarkDown with HTML
<table><thead><tr><th>HTML header</th><th>Value</th></tr></thead><tbody><tr><td>Pomodoros done</td><td>0</td></tr><tr><td>Hours in meetings / Hours worked</td><td>0/0</td></tr></tbody></table>
            """, "html_key"),
            EventMetadataEntry.json({'key': 'value'}, 'json_key'),
            EventMetadataEntry.md("""
# Live demo

Changes are automatically rendered as you type.

* Implements [GitHub Flavored Markdown](https://github.github.com/gfm/)
* Renders actual, "native" React DOM elements
* Allows you to escape or skip HTML (try toggling the checkboxes above)
* If you escape or skip the HTML, no `dangerouslySetInnerHTML` is used! Yay!

## Table of Contents

## HTML block below

<blockquote>
  This blockquote will change based on the HTML settings above.
</blockquote>

## How about some code?

```js
var React = require('react');
var Markdown = require('react-markdown');

React.render(
  <Markdown source="# Your markdown here" />,
  document.getElementById('content')
);
```

Pretty neat, eh?

## Tables?

Use [`remark-gfm`](https://github.com/remarkjs/react-markdown#use) to support tables, strikethrough, tasklists, and literal URLs.
These features **do not work by default**.

| Feature   | Support |
| :-------: | ------- |
| tables    | `remark-gfm` |

~~strikethrough~~

- [ ] task list

https://example.com

## More info?

Read usage information and more on [GitHub](https://github.com/remarkjs/react-markdown)

---------------

A component by [Espen Hovlandsdal](https://espen.codes/)""", "react-markdown-key"),
        ],
    )
    yield Output(value=['element1', 'element2'], output_name="result")
