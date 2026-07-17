ADBC Driver for MonetDB

This distribution contains the following components and third-party dependencies. Their license
texts and the components using each license follow.

{{#each licenses}}
================================================================================
{{{name}}}
================================================================================
{{#each used_by}}
- {{crate.name}} {{crate.version}} {{#if crate.repository}}{{crate.repository}}{{else}}https://crates.io/crates/{{crate.name}}{{/if}}
{{/each}}

{{{text}}}
{{/each}}
